package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/batcher"
	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/server"
)

const validEvent = `{"event_id":"0b5e4a1c-2f4e-4c55-9d2b-4f6a3c1e8a01","event_type":"page_view",` +
	`"user_id":"u-1","ts":"2026-09-21T10:00:00.123Z","properties":{"path":"/"}}`

type fakeInserter struct{}

func (fakeInserter) InsertEvents(context.Context, []event.Event) error { return nil }

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func eventsJSON(n int) string {
	return "[" + strings.Join(slices.Repeat([]string{validEvent}, n), ",") + "]"
}

func newTestServer(t *testing.T, pinger server.Pinger) (http.Handler, *batcher.Batcher) {
	t.Helper()

	reg := prometheus.NewRegistry()
	logger := slog.New(slog.DiscardHandler)
	buf, err := batcher.New(batcher.Config{
		BufferSize:    4,
		BatchSize:     2,
		FlushInterval: time.Hour,
		MaxAttempts:   1,
	}, fakeInserter{}, reg, logger)
	require.NoError(t, err)

	h, err := server.NewHandler(logger, buf, pinger, reg)
	require.NoError(t, err)
	return h, buf
}

func TestIngest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		prefill        int
		body           string
		wantStatus     int
		wantBody       string
		wantRetryAfter string
		wantBuffered   int
	}{
		{
			name:         "accepts batch",
			body:         eventsJSON(2),
			wantStatus:   http.StatusAccepted,
			wantBody:     `{"accepted":2}`,
			wantBuffered: 2,
		},
		{name: "malformed json", body: `[{"event_id":`, wantStatus: http.StatusBadRequest, wantBody: "decode events"},
		{name: "not an array", body: validEvent, wantStatus: http.StatusBadRequest, wantBody: "decode events"},
		{
			name:       "unknown field",
			body:       `[{"event_id":"0b5e4a1c-2f4e-4c55-9d2b-4f6a3c1e8a01","color":"red"}]`,
			wantStatus: http.StatusBadRequest,
			wantBody:   `unknown field \"color\"`,
		},
		{
			name:       "trailing data",
			body:       eventsJSON(1) + eventsJSON(1),
			wantStatus: http.StatusBadRequest,
			wantBody:   "single JSON array",
		},
		{name: "empty array", body: `[]`, wantStatus: http.StatusUnprocessableEntity, wantBody: "no events"},
		{
			name:       "invalid event rejects whole request",
			body:       "[" + validEvent + `,{"event_id":"0b5e4a1c-2f4e-4c55-9d2b-4f6a3c1e8a02","user_id":"u","ts":"2026-09-21T10:00:00Z"}]`,
			wantStatus: http.StatusUnprocessableEntity,
			wantBody:   "event 1: invalid event: event_type is required",
		},
		{
			name:           "buffer full keeps nothing",
			prefill:        3,
			body:           eventsJSON(2),
			wantStatus:     http.StatusTooManyRequests,
			wantBody:       "buffer full",
			wantRetryAfter: "1",
			wantBuffered:   3,
		},
		{
			name:       "more events than buffer",
			body:       eventsJSON(5),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantBody:   "buffer capacity",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, buf := newTestServer(t, fakePinger{})
			require.NoError(t, buf.Enqueue(make([]event.Event, tt.prefill)))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Contains(t, rec.Body.String(), tt.wantBody)
			require.Equal(t, tt.wantRetryAfter, rec.Header().Get("Retry-After"))
			require.Equal(t, tt.wantBuffered, buf.Len())
		})
	}
}

func TestHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pingErr    error
		wantStatus int
		wantBody   string
	}{
		{name: "healthy", wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{
			name:       "clickhouse down",
			pingErr:    errors.New("connection refused"),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"status":"unavailable"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestServer(t, fakePinger{err: tt.pingErr})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.JSONEq(t, tt.wantBody, rec.Body.String())
		})
	}
}

func TestMetrics(t *testing.T) {
	t.Parallel()

	h, buf := newTestServer(t, fakePinger{})
	require.NoError(t, buf.Enqueue(make([]event.Event, 3)))

	for _, body := range []string{eventsJSON(1), eventsJSON(2)} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(body))
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	for _, want := range []string{
		"sink_events_received_total 1",
		`sink_events_rejected_total{reason="buffer_full"} 2`,
		"sink_buffer_length 4",
	} {
		require.Contains(t, string(body), want)
	}
}
