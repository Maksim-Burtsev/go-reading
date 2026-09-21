package server

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func markLimited(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Limited", "true")
		next.ServeHTTP(w, r)
	})
}

func TestServer(t *testing.T) {
	t.Parallel()
	h := New(slog.New(slog.DiscardHandler), markLimited)

	tests := []struct {
		name        string
		method      string
		path        string
		wantStatus  int
		wantBody    string
		wantLimited bool
	}{
		{
			name:       "health check bypasses the limiter",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:        "get proverb",
			method:      http.MethodGet,
			path:        "/api/proverbs/4",
			wantStatus:  http.StatusOK,
			wantBody:    `{"id":4,"text":"Make the zero value useful."}`,
			wantLimited: true,
		},
		{
			name:        "unknown proverb",
			method:      http.MethodGet,
			path:        "/api/proverbs/42",
			wantStatus:  http.StatusNotFound,
			wantBody:    `{"error":"proverb not found"}`,
			wantLimited: true,
		},
		{
			name:        "malformed id",
			method:      http.MethodGet,
			path:        "/api/proverbs/four",
			wantStatus:  http.StatusBadRequest,
			wantBody:    `{"error":"id must be an integer"}`,
			wantLimited: true,
		},
		{
			name:        "wrong method",
			method:      http.MethodDelete,
			path:        "/api/proverbs/4",
			wantStatus:  http.StatusMethodNotAllowed,
			wantLimited: true,
		},
		{
			name:       "unknown route",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil))

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, tt.wantLimited, rec.Header().Get("X-Limited") == "true")
			if tt.wantBody != "" {
				require.JSONEq(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}

func TestListProverbs(t *testing.T) {
	t.Parallel()
	h := New(slog.New(slog.DiscardHandler), markLimited)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/proverbs", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Contains(t, rec.Body.String(), `{"id":1,"text":"Don't communicate by sharing memory, share memory by communicating."}`)
}
