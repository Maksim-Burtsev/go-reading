package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/api"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/status"
)

const maxBody = 256

func TestCreateDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		fillQueue      bool
		closeQueue     bool
		wantCode       int
		wantError      string
		wantRetryAfter string
	}{
		{
			name:     "accepts a delivery",
			body:     `{"url":"https://example.com/hook","payload":{"event":"order.paid"}}`,
			wantCode: http.StatusAccepted,
		},
		{
			name:      "rejects malformed JSON",
			body:      `{"url":`,
			wantCode:  http.StatusBadRequest,
			wantError: "invalid JSON body",
		},
		{
			name:      "rejects unknown fields",
			body:      `{"url":"https://example.com/hook","payload":{},"method":"PUT"}`,
			wantCode:  http.StatusBadRequest,
			wantError: "invalid JSON body",
		},
		{
			name:      "rejects non-http scheme",
			body:      `{"url":"ftp://example.com/hook","payload":{}}`,
			wantCode:  http.StatusBadRequest,
			wantError: "url must be an absolute http or https URL",
		},
		{
			name:      "rejects relative url",
			body:      `{"url":"/hook","payload":{}}`,
			wantCode:  http.StatusBadRequest,
			wantError: "url must be an absolute http or https URL",
		},
		{
			name:      "rejects null payload",
			body:      `{"url":"https://example.com/hook","payload":null}`,
			wantCode:  http.StatusBadRequest,
			wantError: "payload is required",
		},
		{
			name:      "rejects oversized body",
			body:      `{"url":"https://example.com/hook","payload":"` + strings.Repeat("x", maxBody) + `"}`,
			wantCode:  http.StatusRequestEntityTooLarge,
			wantError: "request body too large",
		},
		{
			name:           "sheds load when the queue is full",
			body:           `{"url":"https://example.com/hook","payload":{}}`,
			fillQueue:      true,
			wantCode:       http.StatusServiceUnavailable,
			wantError:      "delivery queue is full",
			wantRetryAfter: "1",
		},
		{
			name:       "refuses deliveries while shutting down",
			body:       `{"url":"https://example.com/hook","payload":{}}`,
			closeQueue: true,
			wantCode:   http.StatusServiceUnavailable,
			wantError:  "server is shutting down",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			queue := dispatch.NewQueue(1)
			if tt.fillQueue {
				require.NoError(t, queue.Push(dispatch.Task{ID: "existing"}))
			}
			if tt.closeQueue {
				queue.Close()
			}
			recorder := status.NewRecorder(time.Hour)
			h := api.NewHandler(queue, recorder, slog.New(slog.DiscardHandler), maxBody)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/deliveries", strings.NewReader(tt.body)))

			require.Equal(t, tt.wantCode, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Equal(t, tt.wantRetryAfter, rec.Header().Get("Retry-After"))
			if tt.wantCode != http.StatusAccepted {
				var body map[string]string
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
				require.Equal(t, tt.wantError, body["error"])
				return
			}

			var got status.Record
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
			require.Equal(t, dispatch.StatusQueued, got.Status)
			require.Equal(t, "https://example.com/hook", got.URL)
			require.Equal(t, "/deliveries/"+got.ID, rec.Header().Get("Location"))

			task := <-queue.Tasks()
			require.Equal(t, got.ID, task.ID)
			require.JSONEq(t, `{"event":"order.paid"}`, string(task.Payload))
		})
	}
}

func TestCreateDeliveryForgetsRejectedTask(t *testing.T) {
	t.Parallel()

	queue := dispatch.NewQueue(1)
	queue.Close()
	recorder := status.NewRecorder(time.Hour)
	var tracked []string
	h := api.NewHandler(queue, spyStore{Recorder: recorder, tracked: &tracked}, slog.New(slog.DiscardHandler), maxBody)

	rec := httptest.NewRecorder()
	body := `{"url":"https://example.com/hook","payload":{}}`
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/deliveries", strings.NewReader(body)))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Len(t, tracked, 1)
	_, ok := recorder.Get(tracked[0])
	require.False(t, ok)
}

type spyStore struct {
	*status.Recorder
	tracked *[]string
}

func (s spyStore) Track(t dispatch.Task) status.Record {
	*s.tracked = append(*s.tracked, t.ID)
	return s.Recorder.Track(t)
}

func TestGetDelivery(t *testing.T) {
	t.Parallel()

	recorder := status.NewRecorder(time.Hour)
	tracked := recorder.Track(dispatch.Task{ID: "known", URL: "https://example.com/hook"})
	h := api.NewHandler(dispatch.NewQueue(1), recorder, slog.New(slog.DiscardHandler), maxBody)

	tests := []struct {
		name     string
		id       string
		wantCode int
	}{
		{name: "known delivery", id: "known", wantCode: http.StatusOK},
		{name: "unknown delivery", id: "missing", wantCode: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/deliveries/"+tt.id, nil))

			require.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode != http.StatusOK {
				return
			}
			var got status.Record
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
			require.Equal(t, tracked.ID, got.ID)
			require.Equal(t, tracked.Status, got.Status)
			require.True(t, tracked.CreatedAt.Equal(got.CreatedAt))
		})
	}
}
