package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/require"
)

func TestClientIP(t *testing.T) {
	t.Parallel()

	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")}

	tests := []struct {
		name       string
		trusted    []netip.Prefix
		remoteAddr string
		xff        string
		want       string
	}{
		{"no trusted proxies: peer", nil, "203.0.113.9:4711", "198.51.100.7", "203.0.113.9"},
		{"trusted proxy: forwarded client", trusted, "10.1.2.3:4711", "198.51.100.7", "198.51.100.7"},
		{"trusted proxy: rightmost untrusted hop", trusted, "10.1.2.3:4711", "6.6.6.6, 198.51.100.7, 10.9.9.9", "198.51.100.7"},
		{"trusted IPv6 proxy", trusted, "[2001:db8::1]:4711", "198.51.100.7", "198.51.100.7"},
		{"trusted proxy as IPv4-mapped IPv6", trusted, "[::ffff:10.1.2.3]:4711", "198.51.100.7", "198.51.100.7"},
		{"untrusted peer: forwarded header ignored", trusted, "203.0.113.9:4711", "6.6.6.6", "203.0.113.9"},
		{"untrusted peer without header", trusted, "203.0.113.9:4711", "", "203.0.113.9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got string
			h := clientIP(tt.trusted)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = middleware.GetClientIP(r.Context())
			}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/users/1", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)

			require.Equal(t, tt.want, got)
		})
	}
}
