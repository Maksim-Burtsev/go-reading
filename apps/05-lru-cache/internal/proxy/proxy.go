// Package proxy implements a read-through HTTP cache in front of a single
// upstream origin.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/lru"
)

// ErrInvalidUpstream is returned by New when the upstream is not an absolute
// http or https URL without a query or fragment.
var ErrInvalidUpstream = errors.New("proxy: invalid upstream URL")

// ErrBodyTooLarge is returned when an upstream response body exceeds the
// configured limit.
var ErrBodyTooLarge = errors.New("proxy: upstream response body too large")

const userAgent = "go-reading-lru-cache/1.0"

// Response is an upstream response held in the cache.
type Response struct {
	status int
	header http.Header
	body   []byte
}

// Cache is the cache a Proxy reads through, keyed by request path and query.
type Cache = lru.Cache[string, *Response]

// Proxy answers GET requests from a cache and fetches misses from the
// upstream, collapsing concurrent misses for the same key into one request.
type Proxy struct {
	base    string
	client  *http.Client
	cache   *Cache
	maxBody int64
	logger  *slog.Logger
	flights singleflight.Group
}

// New returns a Proxy that forwards misses to upstream through client and
// rejects upstream bodies larger than maxBody bytes. The client must have a
// timeout: fetches outlive the requests that started them.
func New(upstream *url.URL, client *http.Client, cache *Cache, maxBody int64, logger *slog.Logger) (*Proxy, error) {
	if upstream.Scheme != "http" && upstream.Scheme != "https" ||
		upstream.Host == "" || upstream.RawQuery != "" || upstream.Fragment != "" {
		return nil, fmt.Errorf("%w: %q", ErrInvalidUpstream, upstream.Redacted())
	}
	return &Proxy{
		base:    strings.TrimSuffix(upstream.String(), "/"),
		client:  client,
		cache:   cache,
		maxBody: maxBody,
		logger:  logger,
	}, nil
}

type snapshotEntry struct {
	Key      string   `json:"key"`
	Response Response `json:"response"`
}

// SaveSnapshot writes the cached responses to w as a stream of JSON objects,
// from the least to the most recently used.
func (p *Proxy) SaveSnapshot(w io.Writer) error {
	enc := json.NewEncoder(w)
	for key, resp := range p.cache.All() {
		if err := enc.Encode(snapshotEntry{Key: key, Response: *resp}); err != nil {
			return fmt.Errorf("encode %q: %w", key, err)
		}
	}
	return nil
}

// LoadSnapshot stores the responses written by SaveSnapshot in the cache and
// returns how many were restored. The whole snapshot is decoded before any
// entry is stored, so a corrupt snapshot leaves the cache untouched.
func (p *Proxy) LoadSnapshot(r io.Reader) (int, error) {
	dec := json.NewDecoder(r)
	var entries []snapshotEntry
	for {
		var e snapshotEntry
		err := dec.Decode(&e)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("decode snapshot entry %d: %w", len(entries), err)
		}
		entries = append(entries, e)
	}
	for _, e := range entries {
		p.cache.Set(e.Key, &e.Response)
	}
	return len(entries), nil
}

// Handler returns the HTTP handler serving proxied requests and the cache
// administration endpoints.
func (p *Proxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_cache/stats", p.handleStats)
	mux.HandleFunc("GET /", p.handleGet)
	mux.HandleFunc("PURGE /", p.handlePurge)
	return mux
}

func (p *Proxy) handleGet(w http.ResponseWriter, r *http.Request) {
	key := requestKey(r.URL)
	resp, hit := p.cache.Get(key)
	if !hit {
		var err error
		if resp, err = p.load(r.Context(), key); err != nil {
			p.writeError(w, r, key, err)
			return
		}
	}
	resp.serve(w, hit)
}

// handlePurge drops the cached response for the request's path and query. A fetch for the same
// key that is already in flight is not affected: it still stores its response when it completes.
func (p *Proxy) handlePurge(w http.ResponseWriter, r *http.Request) {
	if !p.cache.Remove(requestKey(r.URL)) {
		http.Error(w, "not cached", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type statsResponse struct {
	Entries     int     `json:"entries"`
	Hits        uint64  `json:"hits"`
	Misses      uint64  `json:"misses"`
	Evictions   uint64  `json:"evictions"`
	Expirations uint64  `json:"expirations"`
	HitRatio    float64 `json:"hit_ratio"`
}

func (p *Proxy) handleStats(w http.ResponseWriter, r *http.Request) {
	s := p.cache.Stats()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(statsResponse{
		Entries:     p.cache.Len(),
		Hits:        s.Hits,
		Misses:      s.Misses,
		Evictions:   s.Evictions,
		Expirations: s.Expirations,
		HitRatio:    s.HitRatio(),
	})
	if err != nil {
		p.logger.ErrorContext(r.Context(), "encode stats", "error", err)
	}
}

func (p *Proxy) load(ctx context.Context, key string) (*Response, error) {
	ch := p.flights.DoChan(key, func() (any, error) {
		if resp, ok := p.cache.Peek(key); ok {
			return resp, nil
		}
		fetchCtx := context.WithoutCancel(ctx)
		resp, err := p.fetch(fetchCtx, key)
		if err != nil {
			p.logger.ErrorContext(fetchCtx, "upstream fetch failed", "key", key, "error", err)
			return nil, err
		}
		if resp.status == http.StatusOK {
			p.cache.Set(key, resp)
		}
		return resp, nil
	})
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for upstream: %w", ctx.Err())
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*Response), nil
	}
}

func (p *Proxy) fetch(ctx context.Context, key string) (*Response, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+key, http.NoBody) //nolint:gosec // G704: the origin is fixed in New.
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	res, err := p.client.Do(req) //nolint:gosec // G704: the origin is fixed in New.
	if err != nil {
		return nil, fmt.Errorf("upstream request: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(res.Body, p.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("read upstream body: %w", err)
	}
	if int64(len(body)) > p.maxBody {
		return nil, fmt.Errorf("%w: limit is %d bytes", ErrBodyTooLarge, p.maxBody)
	}
	header := make(http.Header, 3)
	for _, name := range [...]string{"Content-Type", "ETag", "Last-Modified"} {
		if v := res.Header.Get(name); v != "" {
			header.Set(name, v)
		}
	}
	p.logger.InfoContext(ctx, "upstream fetch",
		"key", key, "status", res.StatusCode, "bytes", len(body), "duration", time.Since(start))
	return &Response{status: res.StatusCode, header: header, body: body}, nil
}

func (p *Proxy) writeError(w http.ResponseWriter, r *http.Request, key string, err error) {
	if errors.Is(err, context.Canceled) {
		p.logger.InfoContext(r.Context(), "client left before upstream answered", "key", key)
		return
	}
	status := http.StatusBadGateway
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		status = http.StatusGatewayTimeout
	}
	p.logger.InfoContext(r.Context(), "upstream error sent to client", "key", key, "status", status, "error", err)
	http.Error(w, http.StatusText(status), status)
}

func (resp *Response) serve(w http.ResponseWriter, hit bool) {
	h := w.Header()
	for name, values := range resp.header {
		h[name] = slices.Clone(values)
	}
	if hit {
		h.Set("X-Cache", "HIT")
	} else {
		h.Set("X-Cache", "MISS")
	}
	h.Set("Content-Length", strconv.Itoa(len(resp.body)))
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(resp.status)
	_, _ = w.Write(resp.body) //nolint:gosec // G705: relayed verbatim with nosniff.
}

func requestKey(u *url.URL) string {
	if u.RawQuery == "" {
		return u.EscapedPath()
	}
	return u.EscapedPath() + "?" + u.RawQuery
}
