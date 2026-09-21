package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func (s *store[S]) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[key]
	return ok
}

func newCounterStore(t *testing.T, clock Clock, ttl time.Duration) *store[int] {
	t.Helper()
	s, err := newStore(ttl, func(time.Time) int { return 0 }, []Option{WithClock(clock)})
	require.NoError(t, err)
	t.Cleanup(s.close)
	return s
}

func count(t *testing.T, s *store[int], key string) int {
	t.Helper()
	d, err := s.do(key, func(n *int, _ time.Time) Decision {
		*n++
		return Decision{Remaining: *n}
	})
	require.NoError(t, err)
	return d.Remaining
}

func TestStoreEvictsIdleKeys(t *testing.T) {
	t.Parallel()
	const ttl = time.Minute

	tests := []struct {
		name     string
		gaps     []time.Duration
		wantKept bool
	}{
		{name: "just used", gaps: []time.Duration{0}, wantKept: true},
		{name: "idle for less than ttl", gaps: []time.Duration{ttl - time.Nanosecond}, wantKept: true},
		{name: "idle for ttl", gaps: []time.Duration{ttl}, wantKept: false},
		{name: "idle for longer than ttl", gaps: []time.Duration{time.Hour}, wantKept: false},
		{name: "used again within ttl", gaps: []time.Duration{ttl - time.Second, ttl - time.Second}, wantKept: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			s := newCounterStore(t, clock, ttl)

			for _, gap := range tt.gaps {
				count(t, s, "idle")
				clock.Advance(gap)
			}
			count(t, s, "active")
			clock.ticker(t).tickAndWait()

			require.Equal(t, tt.wantKept, s.has("idle"))
			require.True(t, s.has("active"))
		})
	}
}

func TestStoreKeepsStateUntilEvicted(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	s := newCounterStore(t, clock, time.Minute)

	require.Equal(t, 1, count(t, s, "key"))
	require.Equal(t, 2, count(t, s, "key"))

	clock.Advance(time.Minute)
	clock.ticker(t).tickAndWait()
	require.Equal(t, 1, count(t, s, "key"))
}
