package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlidingWindow(t *testing.T) {
	t.Parallel()
	const ms = time.Millisecond
	fourPerSecond := Rate{Limit: 4, Period: time.Second}

	tests := []struct {
		name  string
		rate  Rate
		steps []step
	}{
		{
			name: "limit within one window",
			rate: fourPerSecond,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 2, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 1, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 2 * time.Second}},
				{want: Decision{Limit: 4, Remaining: 0, ResetAfter: 2 * time.Second, RetryAfter: 1250 * ms}},
				{advance: 1250*ms - 1, want: Decision{Limit: 4, Remaining: 0, ResetAfter: 750*ms + 1, RetryAfter: 1}},
				{advance: 1, want: Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 1750 * ms}},
			},
		},
		{
			name: "previous window is weighted by its overlap",
			rate: fourPerSecond,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 2, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 1, ResetAfter: 2 * time.Second}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 2 * time.Second}},
				{advance: 1500 * ms, want: Decision{Allowed: true, Limit: 4, Remaining: 1, ResetAfter: 1500 * ms}},
				{want: Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 1500 * ms}},
				{want: Decision{Limit: 4, Remaining: 0, ResetAfter: 1500 * ms, RetryAfter: 250 * ms}},
				{advance: 250 * ms, want: Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 1250 * ms}},
			},
		},
		{
			name: "window more than a period ago is forgotten",
			rate: fourPerSecond,
			steps: []step{
				{want: Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 2 * time.Second}},
				{advance: 2 * time.Second, want: Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 2 * time.Second}},
			},
		},
		{
			name: "single request per window waits for the previous one to expire",
			rate: Rate{Limit: 1, Period: time.Second},
			steps: []step{
				{advance: 400 * ms, want: Decision{Allowed: true, Limit: 1, Remaining: 0, ResetAfter: 1600 * ms}},
				{advance: 600 * ms, want: Decision{Limit: 1, Remaining: 0, ResetAfter: time.Second, RetryAfter: time.Second}},
				{advance: 999 * ms, want: Decision{Limit: 1, Remaining: 0, ResetAfter: ms, RetryAfter: ms}},
				{advance: ms, want: Decision{Allowed: true, Limit: 1, Remaining: 0, ResetAfter: 2 * time.Second}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			sw, err := NewSlidingWindow(tt.rate, WithClock(clock))
			require.NoError(t, err)
			t.Cleanup(sw.Close)

			runSteps(t, clock, sw, tt.steps)
		})
	}
}

func TestSlidingWindowEvictsAfterTwoPeriods(t *testing.T) {
	t.Parallel()
	sw, err := NewSlidingWindow(Rate{Limit: 4, Period: time.Second}, WithClock(newFakeClock()))
	require.NoError(t, err)
	t.Cleanup(sw.Close)

	require.Equal(t, 2*time.Second, sw.store.ttl)
}
