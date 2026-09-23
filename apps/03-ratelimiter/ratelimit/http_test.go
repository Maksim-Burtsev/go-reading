package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type limiterFunc func(ctx context.Context, key string) (Decision, error)

func (f limiterFunc) Allow(ctx context.Context, key string) (Decision, error) { return f(ctx, key) }

func writeOK(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok")
}

func TestMiddleware(t *testing.T) {
	t.Parallel()
	errKey := errors.New("no key")

	tests := []struct {
		name        string
		keyFunc     KeyFunc
		decision    Decision
		limiterErr  error
		wantKey     string
		wantStatus  int
		wantHeaders map[string]string
		wantBody    string
	}{
		{
			name:       "allowed",
			keyFunc:    RemoteIP,
			decision:   Decision{Allowed: true, Limit: 10, Remaining: 7, ResetAfter: 1500 * time.Millisecond},
			wantKey:    "192.0.2.1",
			wantStatus: http.StatusOK,
			wantHeaders: map[string]string{
				"X-RateLimit-Limit":     "10",
				"X-RateLimit-Remaining": "7",
				"X-RateLimit-Reset":     "2",
				"Retry-After":           "",
			},
			wantBody: "ok",
		},
		{
			name:       "rejected",
			keyFunc:    RemoteIP,
			decision:   Decision{Limit: 10, ResetAfter: 30 * time.Second, RetryAfter: 250 * time.Millisecond},
			wantKey:    "192.0.2.1",
			wantStatus: http.StatusTooManyRequests,
			wantHeaders: map[string]string{
				"Content-Type":          "application/json",
				"X-RateLimit-Limit":     "10",
				"X-RateLimit-Remaining": "0",
				"X-RateLimit-Reset":     "30",
				"Retry-After":           "1",
			},
			wantBody: `{"error":"rate limit exceeded","retry_after_seconds":1}`,
		},
		{
			name:       "limiter failure",
			keyFunc:    RemoteIP,
			limiterErr: ErrClosed,
			wantKey:    "192.0.2.1",
			wantStatus: http.StatusServiceUnavailable,
			wantHeaders: map[string]string{
				"Content-Type":      "application/json",
				"X-RateLimit-Limit": "",
			},
			wantBody: `{"error":"rate limiter unavailable"}`,
		},
		{
			name:       "key failure",
			keyFunc:    func(*http.Request) (string, error) { return "", errKey },
			wantStatus: http.StatusBadRequest,
			wantHeaders: map[string]string{
				"Content-Type":      "application/json",
				"X-RateLimit-Limit": "",
			},
			wantBody: `{"error":"cannot identify client"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotKey string
			l := limiterFunc(func(_ context.Context, key string) (Decision, error) {
				gotKey = key
				return tt.decision, tt.limiterErr
			})
			h := Middleware(l, tt.keyFunc, slog.New(slog.DiscardHandler))(http.HandlerFunc(writeOK))

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

			require.Equal(t, tt.wantKey, gotKey)
			require.Equal(t, tt.wantStatus, rec.Code)
			for name, want := range tt.wantHeaders {
				require.Equal(t, want, rec.Header().Get(name), name)
			}
			if rec.Header().Get("Content-Type") == "application/json" {
				require.JSONEq(t, tt.wantBody, rec.Body.String())
			} else {
				require.Equal(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}

func TestMiddlewareIgnoresEndedRequests(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	l := limiterFunc(func(ctx context.Context, _ string) (Decision, error) {
		return Decision{}, ctx.Err()
	})
	h := Middleware(l, RemoteIP, slog.New(slog.NewJSONHandler(&logs, nil)))(http.HandlerFunc(writeOK))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil))

	require.Empty(t, rec.Header())
	require.Empty(t, rec.Body.String())
	require.Empty(t, logs.String())
}

func TestMiddlewareWithTokenBucket(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	tb, err := NewTokenBucket(Rate{Limit: 1, Period: time.Second}, 2, WithClock(clock))
	require.NoError(t, err)
	t.Cleanup(tb.Close)
	h := Middleware(tb, RemoteIP, slog.New(slog.DiscardHandler))(http.HandlerFunc(writeOK))

	tests := []struct {
		remoteAddr    string
		advance       time.Duration
		wantStatus    int
		wantRemaining string
	}{
		{remoteAddr: "192.0.2.1:1000", wantStatus: http.StatusOK, wantRemaining: "1"},
		{remoteAddr: "192.0.2.1:1001", wantStatus: http.StatusOK, wantRemaining: "0"},
		{remoteAddr: "192.0.2.1:1002", wantStatus: http.StatusTooManyRequests, wantRemaining: "0"},
		{remoteAddr: "192.0.2.2:1000", wantStatus: http.StatusOK, wantRemaining: "1"},
		{remoteAddr: "192.0.2.1:1003", advance: time.Second, wantStatus: http.StatusOK, wantRemaining: "0"},
	}
	for i, tt := range tests {
		clock.Advance(tt.advance)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		req.RemoteAddr = tt.remoteAddr
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, tt.wantStatus, rec.Code, "request %d", i)
		require.Equal(t, tt.wantRemaining, rec.Header().Get("X-RateLimit-Remaining"), "request %d", i)
	}
}

func TestRemoteIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		remoteAddr string
		want       string
		wantErr    error
	}{
		{name: "ipv4", remoteAddr: "192.0.2.1:1234", want: "192.0.2.1"},
		{name: "ipv6", remoteAddr: "[2001:db8::1]:443", want: "2001:db8::1"},
		{name: "ipv4-mapped ipv6", remoteAddr: "[::ffff:192.0.2.1]:80", want: "192.0.2.1"},
		{name: "no port", remoteAddr: "192.0.2.1", wantErr: ErrNoClientIP},
		{name: "empty", remoteAddr: "", wantErr: ErrNoClientIP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr

			got, err := RemoteIP(req)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestForwardedFor(t *testing.T) {
	t.Parallel()
	keyFunc := ForwardedFor([]netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("2001:db8:ffff::/48"),
	})

	tests := []struct {
		name       string
		remoteAddr string
		header     []string
		want       string
		wantErr    error
	}{
		{name: "untrusted peer header is ignored", remoteAddr: "203.0.113.9:1000", header: []string{"198.51.100.1"}, want: "203.0.113.9"},
		{name: "trusted peer without header", remoteAddr: "10.0.0.1:1000", want: "10.0.0.1"},
		{name: "trusted peer", remoteAddr: "10.0.0.1:1000", header: []string{"198.51.100.1"}, want: "198.51.100.1"},
		{name: "client prepended entries are skipped", remoteAddr: "10.0.0.1:1000", header: []string{"1.1.1.1, 198.51.100.1"}, want: "198.51.100.1"},
		{name: "chain of trusted proxies", remoteAddr: "10.0.0.1:1000", header: []string{"198.51.100.1, 10.0.0.7"}, want: "198.51.100.1"},
		{name: "header split across lines", remoteAddr: "10.0.0.1:1000", header: []string{"198.51.100.1", "10.0.0.7"}, want: "198.51.100.1"},
		{name: "every hop trusted", remoteAddr: "10.0.0.1:1000", header: []string{"10.0.0.5,10.0.0.7"}, want: "10.0.0.1"},
		{name: "ipv6 proxy", remoteAddr: "[2001:db8:ffff::1]:443", header: []string{"2001:db8::42"}, want: "2001:db8::42"},
		{name: "malformed hop", remoteAddr: "10.0.0.1:1000", header: []string{"unknown, 10.0.0.7"}, wantErr: ErrNoClientIP},
		{name: "malformed peer", remoteAddr: "10.0.0.1", header: []string{"198.51.100.1"}, wantErr: ErrNoClientIP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for _, v := range tt.header {
				req.Header.Add("X-Forwarded-For", v)
			}

			got, err := keyFunc(req)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}
