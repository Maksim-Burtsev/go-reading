package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucket(t *testing.T) {
	t.Parallel()
	const ms = time.Millisecond
	tenPerSecond := Rate{Limit: 10, Period: time.Second}

	tests := []struct {
		name  string
		rate  Rate
		burst int
		steps []step
	}{
		{
			name:  "burst then refill one token per interval",
			rate:  tenPerSecond,
			burst: 3,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 100 * ms}},
				{want: Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 200 * ms}},
				{want: Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 300 * ms}},
				{want: Decision{Limit: 3, Remaining: 0, ResetAfter: 300 * ms, RetryAfter: 100 * ms}},
				{advance: 100 * ms, want: Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 300 * ms}},
				{advance: 50 * ms, want: Decision{Limit: 3, Remaining: 0, ResetAfter: 250 * ms, RetryAfter: 50 * ms}},
				{advance: 50 * ms, want: Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 300 * ms}},
			},
		},
		{
			name:  "partial interval carries over to the next refill",
			rate:  tenPerSecond,
			burst: 3,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 100 * ms}},
				{want: Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 200 * ms}},
				{want: Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 300 * ms}},
				{advance: 250 * ms, want: Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 150 * ms}},
				{advance: 50 * ms, want: Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 200 * ms}},
			},
		},
		{
			name:  "idle bucket refills up to burst only",
			rate:  tenPerSecond,
			burst: 2,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 2, Remaining: 1, ResetAfter: 100 * ms}},
				{advance: time.Hour, want: Decision{Allowed: true, Limit: 2, Remaining: 1, ResetAfter: 100 * ms}},
				{want: Decision{Allowed: true, Limit: 2, Remaining: 0, ResetAfter: 200 * ms}},
				{want: Decision{Limit: 2, Remaining: 0, ResetAfter: 200 * ms, RetryAfter: 100 * ms}},
			},
		},
		{
			name:  "slow rate without burst",
			rate:  Rate{Limit: 1, Period: time.Minute},
			burst: 1,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 1, Remaining: 0, ResetAfter: time.Minute}},
				{advance: 45 * time.Second, want: Decision{Limit: 1, Remaining: 0, ResetAfter: 15 * time.Second, RetryAfter: 15 * time.Second}},
				{advance: 15 * time.Second, want: Decision{Allowed: true, Limit: 1, Remaining: 0, ResetAfter: time.Minute}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			tb, err := NewTokenBucket(tt.rate, tt.burst, WithClock(clock))
			require.NoError(t, err)
			t.Cleanup(tb.Close)

			runSteps(t, clock, tb, tt.steps)
		})
	}
}

func TestTokenBucketKeysAreIndependent(t *testing.T) {
	t.Parallel()
	tb, err := NewTokenBucket(Rate{Limit: 1, Period: time.Second}, 1, WithClock(newFakeClock()))
	require.NoError(t, err)
	t.Cleanup(tb.Close)

	for _, key := range []string{"a", "b"} {
		d, err := tb.Allow(t.Context(), key)
		require.NoError(t, err)
		require.True(t, d.Allowed, key)
	}
	d, err := tb.Allow(t.Context(), "a")
	require.NoError(t, err)
	require.False(t, d.Allowed)
}

func TestTokenBucketEvictsOnlyFullBuckets(t *testing.T) {
	t.Parallel()
	tb, err := NewTokenBucket(Rate{Limit: 10, Period: time.Second}, 5, WithClock(newFakeClock()))
	require.NoError(t, err)
	t.Cleanup(tb.Close)

	require.Equal(t, 500*time.Millisecond, tb.store.ttl)
}
