package ratelimit

import (
	"fmt"
	"maps"
	"sync"
	"time"
)

type entry[S any] struct {
	state    S
	lastSeen time.Time
}

type store[S any] struct {
	clock    Clock
	ttl      time.Duration
	newState func(now time.Time) S

	mu      sync.Mutex
	entries map[string]*entry[S]
	closed  bool

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newStore[S any](ttl time.Duration, newState func(now time.Time) S, opts []Option) (*store[S], error) {
	o := options{clock: SystemClock{}, cleanupInterval: time.Minute}
	for _, opt := range opts {
		opt(&o)
	}
	if o.clock == nil {
		return nil, fmt.Errorf("%w: clock must not be nil", ErrInvalidConfig)
	}
	if o.cleanupInterval <= 0 {
		return nil, fmt.Errorf("%w: cleanup interval must be positive, got %s", ErrInvalidConfig, o.cleanupInterval)
	}

	s := &store[S]{
		clock:    o.clock,
		ttl:      ttl,
		newState: newState,
		entries:  make(map[string]*entry[S]),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go s.evictLoop(o.clock.NewTicker(o.cleanupInterval))
	return s, nil
}

func (s *store[S]) do(key string, fn func(state *S, now time.Time) Decision) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return Decision{}, ErrClosed
	}
	now := s.clock.Now()
	e, ok := s.entries[key]
	if !ok {
		e = &entry[S]{state: s.newState(now)}
		s.entries[key] = e
	}
	e.lastSeen = now
	return fn(&e.state, now), nil
}

func (s *store[S]) evictLoop(t Ticker) {
	defer close(s.done)
	defer t.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-t.C():
			s.evictIdle()
		}
	}
}

func (s *store[S]) evictIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	maps.DeleteFunc(s.entries, func(_ string, e *entry[S]) bool {
		return now.Sub(e.lastSeen) >= s.ttl
	})
}

func (s *store[S]) close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done

		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		s.entries = nil
	})
}
