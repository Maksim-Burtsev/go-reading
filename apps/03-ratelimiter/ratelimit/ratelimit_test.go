package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) NewTicker(time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTicker{ch: make(chan time.Time)}
	c.tickers = append(c.tickers, t)
	return t
}

func (c *fakeClock) ticker(t *testing.T) *fakeTicker {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.tickers, 1)
	return c.tickers[0]
}

type fakeTicker struct {
	ch      chan time.Time
	stopped atomic.Bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }

func (t *fakeTicker) Stop() { t.stopped.Store(true) }

func (t *fakeTicker) tickAndWait() {
	t.ch <- time.Time{}
	t.ch <- time.Time{}
}

type step struct {
	advance time.Duration
	want    Decision
}

func runSteps(t *testing.T, clock *fakeClock, l Limiter, steps []step) {
	t.Helper()
	for i, s := range steps {
		clock.Advance(s.advance)
		got, err := l.Allow(t.Context(), "client")
		require.NoError(t, err)
		require.Equal(t, s.want, got, "step %d", i)
	}
}
