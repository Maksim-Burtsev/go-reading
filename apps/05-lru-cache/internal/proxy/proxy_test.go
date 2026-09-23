package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/internal/proxy"
	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/lru"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newProxy(t *testing.T, upstream string, client *http.Client, maxBody int64) (http.Handler, *proxy.Cache) {
	t.Helper()
	u, err := url.Parse(upstream)
	require.NoError(t, err)
	cache, err := lru.New[string, *proxy.Response](16)
	require.NoError(t, err)
	p, err := proxy.New(u, client, cache, maxBody, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	return p.Handler(), cache
}

func serve(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, target, nil))
	return rec
}

func newUpstream(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("ETag", `"v1"`)
			w.Header().Set("Set-Cookie", "session=secret")
			_, _ = io.WriteString(w, "hello")
		case "/big":
			_, _ = io.WriteString(w, strings.Repeat("x", 64))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type probe struct {
	target    string
	wantCache string
}

func TestNewRejectsInvalidUpstream(t *testing.T) {
	t.Parallel()
	cache, err := lru.New[string, *proxy.Response](1)
	require.NoError(t, err)
	for _, raw := range []string{"ftp://example.com", "/relative", "http:///path", "http://h?q=1", "http://h#frag"} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, err = proxy.New(u, http.DefaultClient, cache, 1, slog.New(slog.DiscardHandler))
		require.ErrorIs(t, err, proxy.ErrInvalidUpstream, raw)
	}
}

func TestReadThrough(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		maxBody    int64
		probes     []probe
		wantStatus int
		wantBody   string
		wantCalls  int32
	}{
		{
			name:       "ok response is cached",
			maxBody:    1024,
			probes:     []probe{{"/ok", "MISS"}, {"/ok", "HIT"}},
			wantStatus: http.StatusOK,
			wantBody:   "hello",
			wantCalls:  1,
		},
		{
			name:       "query is part of the key",
			maxBody:    1024,
			probes:     []probe{{"/ok?v=1", "MISS"}, {"/ok?v=2", "MISS"}, {"/ok?v=1", "HIT"}},
			wantStatus: http.StatusOK,
			wantBody:   "hello",
			wantCalls:  2,
		},
		{
			name:       "error response is relayed but not cached",
			maxBody:    1024,
			probes:     []probe{{"/missing", "MISS"}, {"/missing", "MISS"}},
			wantStatus: http.StatusNotFound,
			wantBody:   "404 page not found\n",
			wantCalls:  2,
		},
		{
			name:       "oversized body is rejected",
			maxBody:    4,
			probes:     []probe{{"/big", ""}, {"/big", ""}},
			wantStatus: http.StatusBadGateway,
			wantBody:   "Bad Gateway\n",
			wantCalls:  2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			upstream := newUpstream(t, &calls)
			h, _ := newProxy(t, upstream.URL, upstream.Client(), tt.maxBody)

			for _, pr := range tt.probes {
				rec := serve(t, h, http.MethodGet, pr.target)
				require.Equal(t, tt.wantStatus, rec.Code)
				require.Equal(t, tt.wantBody, rec.Body.String())
				require.Equal(t, pr.wantCache, rec.Header().Get("X-Cache"))
				require.Empty(t, rec.Header().Get("Set-Cookie"))
			}
			require.Equal(t, tt.wantCalls, calls.Load())
		})
	}
}

func TestCachedResponseKeepsHeaders(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := newUpstream(t, &calls)
	h, _ := newProxy(t, upstream.URL, upstream.Client(), 1024)

	serve(t, h, http.MethodGet, "/ok")
	rec := serve(t, h, http.MethodGet, "/ok")
	require.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	require.Equal(t, `"v1"`, rec.Header().Get("ETag"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "5", rec.Header().Get("Content-Length"))
}

func TestPurge(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := newUpstream(t, &calls)
	h, _ := newProxy(t, upstream.URL, upstream.Client(), 1024)

	require.Equal(t, "MISS", serve(t, h, http.MethodGet, "/ok").Header().Get("X-Cache"))
	require.Equal(t, http.StatusNoContent, serve(t, h, "PURGE", "/ok").Code)
	require.Equal(t, http.StatusNotFound, serve(t, h, "PURGE", "/ok").Code)
	require.Equal(t, "MISS", serve(t, h, http.MethodGet, "/ok").Header().Get("X-Cache"))
	require.Equal(t, int32(2), calls.Load())
}

func TestStats(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := newUpstream(t, &calls)
	h, _ := newProxy(t, upstream.URL, upstream.Client(), 1024)

	serve(t, h, http.MethodGet, "/ok")
	serve(t, h, http.MethodGet, "/ok")
	rec := serve(t, h, http.MethodGet, "/_cache/stats")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.JSONEq(t,
		`{"entries":1,"hits":1,"misses":1,"evictions":0,"expirations":0,"hit_ratio":0.5}`,
		rec.Body.String())
}

func TestUpstreamErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		transport  roundTripFunc
		wantStatus int
	}{
		{
			name: "timeout",
			transport: func(r *http.Request) (*http.Response, error) {
				<-r.Context().Done()
				return nil, r.Context().Err()
			},
			wantStatus: http.StatusGatewayTimeout,
		},
		{
			name: "connection failure",
			transport: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			},
			wantStatus: http.StatusBadGateway,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				client := &http.Client{Transport: tt.transport, Timeout: 5 * time.Second}
				h, cache := newProxy(t, "http://upstream.test", client, 1024)
				rec := serve(t, h, http.MethodGet, "/x")
				require.Equal(t, tt.wantStatus, rec.Code)
				require.Zero(t, cache.Len())
			})
		})
	}
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) entries(t *testing.T) []string {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var entries []string
	for line := range strings.Lines(b.buf.String()) {
		var e struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &e))
		entries = append(entries, e.Level+" "+e.Msg)
	}
	return entries
}

func TestUpstreamFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		waiters    int
		leaveAfter time.Duration
		want       []string
	}{
		{
			name:    "every waiter is answered",
			waiters: 3,
			want: []string{
				"ERROR upstream fetch failed",
				"INFO upstream error sent to client",
				"INFO upstream error sent to client",
				"INFO upstream error sent to client",
			},
		},
		{
			name:       "the only client left first",
			waiters:    1,
			leaveAfter: 5 * time.Second,
			want: []string{
				"INFO client left before upstream answered",
				"ERROR upstream fetch failed",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var logs logBuffer
				hang := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				})
				u, err := url.Parse("http://upstream.test")
				require.NoError(t, err)
				cache, err := lru.New[string, *proxy.Response](16)
				require.NoError(t, err)
				client := &http.Client{Transport: hang, Timeout: 10 * time.Second}
				p, err := proxy.New(u, client, cache, 1024, slog.New(slog.NewJSONHandler(&logs, nil)))
				require.NoError(t, err)
				h := p.Handler()

				var wg sync.WaitGroup
				for range tt.waiters {
					wg.Go(func() {
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						if tt.leaveAfter > 0 {
							time.AfterFunc(tt.leaveAfter, cancel)
						}
						h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/x", nil))
					})
				}
				wg.Wait()
				time.Sleep(time.Minute)
				synctest.Wait()

				require.ElementsMatch(t, tt.want, logs.entries(t))
			})
		})
	}
}

func blockingTransport(release <-chan struct{}, calls *atomic.Int32) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-release:
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/plain"}},
			Body:       io.NopCloser(strings.NewReader("payload")),
			Request:    r,
		}, nil
	}
}

func TestConcurrentMissesShareOneFetch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		client := &http.Client{Transport: blockingTransport(release, &calls), Timeout: time.Minute}
		h, _ := newProxy(t, "http://upstream.test", client, 1024)

		recs := make([]*httptest.ResponseRecorder, 10)
		var wg sync.WaitGroup
		for i := range recs {
			wg.Go(func() { recs[i] = serve(t, h, http.MethodGet, "/slow") })
		}
		synctest.Wait()
		close(release)
		wg.Wait()

		require.Equal(t, int32(1), calls.Load())
		for _, rec := range recs {
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "payload", rec.Body.String())
		}
	})
}

func TestFetchOutlivesCanceledRequest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		client := &http.Client{Transport: blockingTransport(release, &calls), Timeout: time.Minute}
		h, cache := newProxy(t, "http://upstream.test", client, 1024)

		ctx, cancel := context.WithCancel(t.Context())
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/slow", nil))
		}()
		synctest.Wait()
		cancel()
		<-done
		require.Empty(t, rec.Body.String())

		close(release)
		synctest.Wait()
		_, ok := cache.Peek("/slow")
		require.True(t, ok)
		require.Equal(t, "HIT", serve(t, h, http.MethodGet, "/slow").Header().Get("X-Cache"))
		require.Equal(t, int32(1), calls.Load())
	})
}

func TestPurgeDoesNotAffectFetchInFlight(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		client := &http.Client{Transport: blockingTransport(release, &calls), Timeout: time.Minute}
		h, cache := newProxy(t, "http://upstream.test", client, 1024)

		done := make(chan struct{})
		go func() {
			defer close(done)
			serve(t, h, http.MethodGet, "/slow")
		}()
		synctest.Wait()
		require.Equal(t, http.StatusNotFound, serve(t, h, "PURGE", "/slow").Code)

		close(release)
		<-done
		_, ok := cache.Peek("/slow")
		require.True(t, ok)
	})
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := newUpstream(t, &calls)
	u, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	newCachedProxy := func() (*proxy.Proxy, *proxy.Cache) {
		cache, err := lru.New[string, *proxy.Response](16)
		require.NoError(t, err)
		p, err := proxy.New(u, upstream.Client(), cache, 1024, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		return p, cache
	}

	src, _ := newCachedProxy()
	serve(t, src.Handler(), http.MethodGet, "/ok")
	serve(t, src.Handler(), http.MethodGet, "/ok?v=2")
	var snapshot bytes.Buffer
	require.NoError(t, src.SaveSnapshot(&snapshot))

	dst, cache := newCachedProxy()
	n, err := dst.LoadSnapshot(&snapshot)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 2, cache.Len())
	for _, key := range []string{"/ok", "/ok?v=2"} {
		_, ok := cache.Peek(key)
		require.True(t, ok, key)
	}
	require.Equal(t, int32(2), calls.Load())
}

func TestLoadSnapshotRejectsCorruptInput(t *testing.T) {
	t.Parallel()
	u, err := url.Parse("http://upstream.test")
	require.NoError(t, err)
	cache, err := lru.New[string, *proxy.Response](16)
	require.NoError(t, err)
	p, err := proxy.New(u, http.DefaultClient, cache, 1024, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	_, err = p.LoadSnapshot(strings.NewReader(`{"key":"/a","response":{}}` + "\n" + `{"key":`))
	require.Error(t, err)
	require.Zero(t, cache.Len())
}
