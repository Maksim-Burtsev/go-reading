package httpapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newCapturingLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil))), &buf
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	sc := bufio.NewScanner(buf)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
		records = append(records, rec)
	}
	require.NoError(t, sc.Err())
	return records
}

func TestRequestID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header   string
		wantEcho bool
	}{
		{name: "propagates client id", header: "req-42.abc_DEF", wantEcho: true},
		{name: "generates when missing", header: ""},
		{name: "replaces oversized id", header: strings.Repeat("a", maxRequestIDBytes+1)},
		{name: "replaces id with spaces", header: "req 42"},
		{name: "replaces non-ascii id", header: "запрос"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger, buf := newCapturingLogger()
			a := &api{logger: logger}

			var seen string
			h := withRequestID(a.logRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen, _ = requestIDFromContext(r.Context())
				logger.With(slog.String("component", "test")).InfoContext(r.Context(), "inside handler")
				w.WriteHeader(http.StatusTeapot)
			})))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set(requestIDHeader, tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(requestIDHeader)
			require.Equal(t, seen, got)
			if tt.wantEcho {
				require.Equal(t, tt.header, got)
			} else {
				require.NoError(t, uuid.Validate(got))
			}

			records := logRecords(t, buf)
			require.Len(t, records, 2)
			for _, r := range records {
				require.Equal(t, got, r["request_id"])
			}
			require.Equal(t, "test", records[0]["component"])
			require.Equal(t, "http request", records[1]["msg"])
			require.InDelta(t, http.StatusTeapot, records[1]["status"], 0)
		})
	}
}

func TestRecoverPanics(t *testing.T) {
	t.Parallel()

	logger, buf := newCapturingLogger()
	a := &api{logger: logger}
	h := withRequestID(a.logRequests(a.recoverPanics(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/notes", nil)
	req.Header.Set(requestIDHeader, "panic-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "internal", decodeBody[errorResponse](t, rec).Error.Code)

	records := logRecords(t, buf)
	require.Len(t, records, 2)

	panicRecord := records[0]
	require.Equal(t, "panic recovered", panicRecord["msg"])
	require.Equal(t, "ERROR", panicRecord["level"])
	require.Equal(t, "boom", panicRecord["panic"])
	require.Equal(t, "panic-1", panicRecord["request_id"])
	require.Contains(t, panicRecord["stack"], "recoverPanic")

	require.InDelta(t, http.StatusInternalServerError, records[1]["status"], 0)
}

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus int
		wantBody   string
	}{
		{
			name: "fast handler",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"ok":true}`))
			},
			wantStatus: http.StatusCreated,
			wantBody:   `{"ok":true}`,
		},
		{
			name: "slow handler",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"timeout","message":"request timed out"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := withTimeout(tt.handler, 50*time.Millisecond)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.JSONEq(t, tt.wantBody, rec.Body.String())
		})
	}
}
