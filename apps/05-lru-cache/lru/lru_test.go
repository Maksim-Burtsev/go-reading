package lru_test

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/lru"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
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

type evicted struct {
	key    string
	reason lru.EvictReason
}

func newCache(tb testing.TB, capacity int, opts ...lru.Option[string, int]) *lru.Cache[string, int] {
	tb.Helper()
	c, err := lru.New(capacity, opts...)
	require.NoError(tb, err)
	return c
}

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{0, -1} {
		_, err := lru.New[string, int](capacity)
		require.ErrorIs(t, err, lru.ErrInvalidCapacity)
	}
}

func TestEvictions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		capacity int
		ttl      time.Duration
		steps    func(c *lru.Cache[string, int], clock *fakeClock)
		want     []evicted
		wantLen  int
	}{
		{
			name:     "capacity evicts the least recently set entry",
			capacity: 2,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Set("b", 2)
				c.Set("c", 3)
			},
			want:    []evicted{{"a", lru.ReasonCapacity}},
			wantLen: 2,
		},
		{
			name:     "get refreshes recency",
			capacity: 2,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Set("b", 2)
				c.Get("a")
				c.Set("c", 3)
			},
			want:    []evicted{{"b", lru.ReasonCapacity}},
			wantLen: 2,
		},
		{
			name:     "peek keeps recency",
			capacity: 2,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Set("b", 2)
				c.Peek("a")
				c.Set("c", 3)
			},
			want:    []evicted{{"a", lru.ReasonCapacity}},
			wantLen: 2,
		},
		{
			name:     "replacing a value refreshes recency without a callback",
			capacity: 2,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Set("b", 2)
				c.Set("a", 10)
				c.Set("c", 3)
			},
			want:    []evicted{{"b", lru.ReasonCapacity}},
			wantLen: 2,
		},
		{
			name:     "remove",
			capacity: 2,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Remove("a")
				c.Remove("missing")
			},
			want:    []evicted{{"a", lru.ReasonRemoved}},
			wantLen: 0,
		},
		{
			name:     "purge reports oldest first",
			capacity: 3,
			steps: func(c *lru.Cache[string, int], _ *fakeClock) {
				c.Set("a", 1)
				c.Set("b", 2)
				c.Set("c", 3)
				c.Purge()
			},
			want:    []evicted{{"a", lru.ReasonRemoved}, {"b", lru.ReasonRemoved}, {"c", lru.ReasonRemoved}},
			wantLen: 0,
		},
		{
			name:     "get removes an expired entry",
			capacity: 2,
			ttl:      time.Minute,
			steps: func(c *lru.Cache[string, int], clock *fakeClock) {
				c.Set("a", 1)
				clock.Advance(time.Minute)
				c.Get("a")
			},
			want:    []evicted{{"a", lru.ReasonExpired}},
			wantLen: 0,
		},
		{
			name:     "delete expired keeps live entries",
			capacity: 3,
			ttl:      time.Minute,
			steps: func(c *lru.Cache[string, int], clock *fakeClock) {
				c.Set("a", 1)
				clock.Advance(30 * time.Second)
				c.Set("b", 2)
				clock.Advance(30 * time.Second)
				c.DeleteExpired()
			},
			want:    []evicted{{"a", lru.ReasonExpired}},
			wantLen: 1,
		},
		{
			name:     "expired oldest entry pushed out by capacity counts as expired",
			capacity: 2,
			ttl:      time.Minute,
			steps: func(c *lru.Cache[string, int], clock *fakeClock) {
				c.Set("a", 1)
				clock.Advance(time.Minute)
				c.Set("b", 2)
				c.Set("c", 3)
			},
			want:    []evicted{{"a", lru.ReasonExpired}},
			wantLen: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			var got []evicted
			c := newCache(t, tt.capacity,
				lru.WithTTL[string, int](tt.ttl),
				lru.WithClock[string, int](clock.Now),
				lru.WithOnEvict(func(key string, _ int, reason lru.EvictReason) {
					got = append(got, evicted{key, reason})
				}),
			)
			tt.steps(c, clock)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantLen, c.Len())
		})
	}
}

func TestExpiry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		ttl    time.Duration
		steps  func(c *lru.Cache[string, int], clock *fakeClock)
		wantOK bool
	}{
		{
			name: "fresh entry is returned",
			ttl:  time.Minute,
			steps: func(_ *lru.Cache[string, int], clock *fakeClock) {
				clock.Advance(time.Minute - time.Nanosecond)
			},
			wantOK: true,
		},
		{
			name: "entry expires at its deadline",
			ttl:  time.Minute,
			steps: func(_ *lru.Cache[string, int], clock *fakeClock) {
				clock.Advance(time.Minute)
			},
			wantOK: false,
		},
		{
			name: "set resets the deadline",
			ttl:  time.Minute,
			steps: func(c *lru.Cache[string, int], clock *fakeClock) {
				clock.Advance(50 * time.Second)
				c.Set("k", 2)
				clock.Advance(50 * time.Second)
			},
			wantOK: true,
		},
		{
			name: "get does not extend the deadline",
			ttl:  time.Minute,
			steps: func(c *lru.Cache[string, int], clock *fakeClock) {
				clock.Advance(50 * time.Second)
				c.Get("k")
				clock.Advance(10 * time.Second)
			},
			wantOK: false,
		},
		{
			name: "zero ttl never expires",
			steps: func(_ *lru.Cache[string, int], clock *fakeClock) {
				clock.Advance(24 * time.Hour)
			},
			wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			c := newCache(t, 4, lru.WithTTL[string, int](tt.ttl), lru.WithClock[string, int](clock.Now))
			c.Set("k", 1)
			tt.steps(c, clock)
			_, peeked := c.Peek("k")
			_, ok := c.Get("k")
			require.Equal(t, tt.wantOK, peeked)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	c := newCache(t, 2, lru.WithTTL[string, int](time.Minute), lru.WithClock[string, int](clock.Now))

	c.Set("a", 1)
	c.Get("a")
	c.Get("missing")
	c.Set("b", 2)
	c.Set("c", 3)
	clock.Advance(time.Minute)
	c.Peek("c")
	c.Get("b")

	s := c.Stats()
	require.Equal(t, lru.Stats{Hits: 1, Misses: 2, Evictions: 1, Expirations: 1}, s)
	require.InDelta(t, 1.0/3.0, s.HitRatio(), 1e-9)
	require.Equal(t, 1, c.Len())
	require.Equal(t, 1, c.DeleteExpired())
	require.Equal(t, 0, c.Len())
}

func TestHitRatio(t *testing.T) {
	t.Parallel()
	tests := []struct {
		stats lru.Stats
		want  float64
	}{
		{lru.Stats{}, 0},
		{lru.Stats{Hits: 3, Misses: 1}, 0.75},
		{lru.Stats{Misses: 5}, 0},
	}
	for _, tt := range tests {
		require.InDelta(t, tt.want, tt.stats.HitRatio(), 1e-9)
	}
}

func TestEvictReasonString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		reason lru.EvictReason
		want   string
	}{
		{lru.ReasonCapacity, "capacity"},
		{lru.ReasonExpired, "expired"},
		{lru.ReasonRemoved, "removed"},
		{lru.EvictReason(0), "EvictReason(0)"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.reason.String())
	}
}

func TestOnEvictMayCallBackIntoCache(t *testing.T) {
	t.Parallel()
	var (
		c         *lru.Cache[string, int]
		lenInside int
	)
	c = newCache(t, 1, lru.WithOnEvict(func(string, int, lru.EvictReason) {
		lenInside = c.Len()
		c.Peek("b")
	}))
	c.Set("a", 1)
	c.Set("b", 2)
	require.Equal(t, 1, lenInside)
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	const (
		capacity = 64
		workers  = 16
		ops      = 2000
		keys     = 200
	)
	clock := newFakeClock()
	var callbacks atomic.Uint64
	c := newCache(t, capacity,
		lru.WithTTL[string, int](time.Second),
		lru.WithClock[string, int](clock.Now),
		lru.WithOnEvict(func(string, int, lru.EvictReason) { callbacks.Add(1) }),
	)

	var gets, removed atomic.Uint64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range ops {
				key := strconv.Itoa((w*ops + i) % keys)
				switch i % 6 {
				case 0, 1:
					c.Set(key, i)
				case 2:
					c.Get(key)
					gets.Add(1)
				case 3:
					c.Peek(key)
				case 4:
					if c.Remove(key) {
						removed.Add(1)
					}
				case 5:
					clock.Advance(10 * time.Millisecond)
					c.DeleteExpired()
				}
			}
		})
	}
	wg.Wait()

	s := c.Stats()
	require.LessOrEqual(t, c.Len(), capacity)
	require.Equal(t, gets.Load(), s.Hits+s.Misses)
	require.Equal(t, callbacks.Load(), s.Evictions+s.Expirations+removed.Load())
}
