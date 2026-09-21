package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrNoClientIP is returned by key functions when the request carries no usable client address.
var ErrNoClientIP = errors.New("ratelimit: no client IP")

// Limiter decides whether a request identified by key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// KeyFunc returns the key a request is rate limited by.
type KeyFunc func(r *http.Request) (string, error)

// Middleware returns an HTTP middleware that rate limits requests by the key from keyFunc and
// sets the X-RateLimit-Limit, X-RateLimit-Remaining and X-RateLimit-Reset headers, in seconds.
// Rejected requests get 429 with Retry-After and a JSON body; key and limiter failures get 400
// and 503.
func Middleware(l Limiter, keyFunc KeyFunc, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &limitHandler{limiter: l, keyFunc: keyFunc, logger: logger, next: next}
	}
}

type limitHandler struct {
	limiter Limiter
	keyFunc KeyFunc
	logger  *slog.Logger
	next    http.Handler
}

type errorResponse struct {
	Error      string `json:"error"`
	RetryAfter int64  `json:"retry_after_seconds,omitempty"`
}

func (h *limitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	key, err := h.keyFunc(r)
	if err != nil {
		h.logger.WarnContext(ctx, "rate limit key", slog.Any("error", err))
		h.writeError(ctx, w, http.StatusBadRequest, errorResponse{Error: "cannot identify client"})
		return
	}

	d, err := h.limiter.Allow(ctx, key)
	if err != nil {
		h.logger.ErrorContext(ctx, "rate limit decision", slog.String("key", key), slog.Any("error", err))
		h.writeError(ctx, w, http.StatusServiceUnavailable, errorResponse{Error: "rate limiter unavailable"})
		return
	}

	header := w.Header()
	header.Set("X-RateLimit-Limit", strconv.Itoa(d.Limit))
	header.Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
	header.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
	if !d.Allowed {
		retryAfter := ceilSeconds(d.RetryAfter)
		header.Set("Retry-After", strconv.FormatInt(retryAfter, 10))
		h.writeError(ctx, w, http.StatusTooManyRequests, errorResponse{Error: "rate limit exceeded", RetryAfter: retryAfter})
		return
	}
	h.next.ServeHTTP(w, r)
}

func (h *limitHandler) writeError(ctx context.Context, w http.ResponseWriter, status int, body errorResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		h.logger.ErrorContext(ctx, "write rate limit response", slog.Any("error", err))
	}
}

func ceilSeconds(d time.Duration) int64 {
	return int64((d + time.Second - 1) / time.Second)
}

// RemoteIP keys requests by the IP address of the direct peer. Forwarding headers are ignored,
// so behind a reverse proxy every request shares the key of the proxy; use ForwardedFor there.
func RemoteIP(r *http.Request) (string, error) {
	addr, err := remoteAddr(r)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

// ForwardedFor keys requests by the client IP reported in X-Forwarded-For, provided the direct
// peer is within one of the trusted proxy prefixes. The header is read from right to left and
// the first address outside the trusted prefixes is the key, so addresses a client prepends to
// the header itself are never used. Requests from untrusted peers are keyed as by RemoteIP.
func ForwardedFor(trusted []netip.Prefix) KeyFunc {
	isTrusted := func(addr netip.Addr) bool {
		return slices.ContainsFunc(trusted, func(p netip.Prefix) bool { return p.Contains(addr) })
	}

	return func(r *http.Request) (string, error) {
		peer, err := remoteAddr(r)
		if err != nil {
			return "", err
		}
		if !isTrusted(peer) {
			return peer.String(), nil
		}

		hops := strings.FieldsFunc(strings.Join(r.Header.Values("X-Forwarded-For"), ","), func(c rune) bool {
			return c == ',' || c == ' '
		})
		for _, hop := range slices.Backward(hops) {
			addr, err := netip.ParseAddr(hop)
			if err != nil {
				return "", fmt.Errorf("%w: X-Forwarded-For entry %q: %w", ErrNoClientIP, hop, err)
			}
			if addr = addr.Unmap(); !isTrusted(addr) {
				return addr.String(), nil
			}
		}
		return peer.String(), nil
	}
}

func remoteAddr(r *http.Request) (netip.Addr, error) {
	addrPort, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%w: remote address %q: %w", ErrNoClientIP, r.RemoteAddr, err)
	}
	return addrPort.Addr().Unmap(), nil
}
