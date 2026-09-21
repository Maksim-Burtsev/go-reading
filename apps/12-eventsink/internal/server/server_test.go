package server_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/server"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

type blockingPinger struct{}

func (blockingPinger) Ping(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deps       map[string]server.Pinger
		wantStatus int
		wantBody   string
	}{
		{
			name:       "all dependencies up",
			deps:       map[string]server.Pinger{"clickhouse": fakePinger{}, "postgres": fakePinger{}, "kafka": fakePinger{}},
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok","dependencies":{"clickhouse":"ok","kafka":"ok","postgres":"ok"}}`,
		},
		{
			name: "one dependency down",
			deps: map[string]server.Pinger{
				"clickhouse": fakePinger{},
				"postgres":   fakePinger{err: errors.New("connection refused")},
				"kafka":      fakePinger{},
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"status":"unavailable","dependencies":{"clickhouse":"ok","kafka":"ok","postgres":"connection refused"}}`,
		},
		{
			name:       "hanging dependency times out",
			deps:       map[string]server.Pinger{"kafka": blockingPinger{}},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"status":"unavailable","dependencies":{"kafka":"context deadline exceeded"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := server.NewHandler(slog.New(slog.DiscardHandler), prometheus.NewRegistry(), tt.deps)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.JSONEq(t, tt.wantBody, rec.Body.String())
		})
	}
}

func TestMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "eventsink_test_total", Help: "Test counter."})
	require.NoError(t, reg.Register(counter))
	counter.Add(3)

	h := server.NewHandler(slog.New(slog.DiscardHandler), reg, nil)
	for _, tt := range []struct {
		method     string
		wantStatus int
	}{
		{method: http.MethodGet, wantStatus: http.StatusOK},
		{method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), tt.method, "/metrics", nil))
		require.Equal(t, tt.wantStatus, rec.Code, tt.method)
		if tt.wantStatus == http.StatusOK {
			require.Contains(t, rec.Body.String(), "eventsink_test_total 3")
		}
	}
}
