package dispatch

import (
	"context"
	"net/url"
	"sync"
)

type hostLimiter struct {
	limit int

	mu    sync.Mutex
	slots map[string]chan struct{}
}

func newHostLimiter(limit int) *hostLimiter {
	l := hostLimiter{limit: limit, slots: make(map[string]chan struct{})}
	return &l
}

// acquire blocks until host has a free slot or ctx is done. The returned
// func gives the slot back.
func (l *hostLimiter) acquire(ctx context.Context, host string) (func(), error) {
	l.mu.Lock()
	sem, ok := l.slots[host]
	if !ok {
		sem = make(chan struct{}, l.limit)
		l.slots[host] = sem
	}
	l.mu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Host
}
