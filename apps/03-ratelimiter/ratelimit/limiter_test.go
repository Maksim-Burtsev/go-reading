package ratelimit

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type closableLimiter interface {
	Limiter
	Close()
}

type limiterCase struct {
	name string
	new  func(Clock) (closableLimiter, error)
}

func limiterCases(limit int) []limiterCase {
	return []limiterCase{
		{
			name: "token bucket",
			new: func(c Clock) (closableLimiter, error) {
				return NewTokenBucket(Rate{Limit: 1, Period: time.Hour}, limit, WithClock(c))
			},
		},
		{
			name: "sliding window",
			new: func(c Clock) (closableLimiter, error) {
				return NewSlidingWindow(Rate{Limit: limit, Period: time.Hour}, WithClock(c))
			},
		},
	}
}

func TestAllowConcurrent(t *testing.T) {
	t.Parallel()
	const (
		limit      = 25
		keys       = 4
		goroutines = 50
		calls      = 20
	)

	for _, tt := range limiterCases(limit) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			l, err := tt.new(clock)
			require.NoError(t, err)
			t.Cleanup(l.Close)
			ticker := clock.ticker(t)

			var allowed [keys]atomic.Int64
			var failed atomic.Int64
			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Go(func() {
					for i := range calls {
						k := (g + i) % keys
						d, err := l.Allow(t.Context(), "client-"+strconv.Itoa(k))
						switch {
						case err != nil:
							failed.Add(1)
						case d.Allowed:
							allowed[k].Add(1)
						}
					}
				})
			}
			wg.Go(func() {
				for range 5 {
					ticker.tickAndWait()
				}
			})
			wg.Wait()

			require.Zero(t, failed.Load())
			for k := range keys {
				require.EqualValues(t, limit, allowed[k].Load(), "key %d", k)
			}
		})
	}
}

func TestAllowAfterClose(t *testing.T) {
	t.Parallel()
	for _, tt := range limiterCases(10) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			l, err := tt.new(clock)
			require.NoError(t, err)

			_, err = l.Allow(t.Context(), "client")
			require.NoError(t, err)
			l.Close()
			l.Close()

			_, err = l.Allow(t.Context(), "client")
			require.ErrorIs(t, err, ErrClosed)
			require.True(t, clock.ticker(t).stopped.Load())
		})
	}
}

func TestAllowCanceledContext(t *testing.T) {
	t.Parallel()
	for _, tt := range limiterCases(10) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l, err := tt.new(newFakeClock())
			require.NoError(t, err)
			t.Cleanup(l.Close)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = l.Allow(ctx, "client")
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestInvalidConfig(t *testing.T) {
	t.Parallel()
	perSecond := Rate{Limit: 10, Period: time.Second}
	tests := []struct {
		name string
		new  func() (closableLimiter, error)
	}{
		{
			name: "zero limit",
			new:  func() (closableLimiter, error) { return NewTokenBucket(Rate{Period: time.Second}, 1) },
		},
		{
			name: "negative period",
			new:  func() (closableLimiter, error) { return NewSlidingWindow(Rate{Limit: 1, Period: -time.Second}) },
		},
		{
			name: "zero burst",
			new:  func() (closableLimiter, error) { return NewTokenBucket(perSecond, 0) },
		},
		{
			name: "refill faster than one token per nanosecond",
			new:  func() (closableLimiter, error) { return NewTokenBucket(Rate{Limit: 2, Period: time.Nanosecond}, 1) },
		},
		{
			name: "zero cleanup interval",
			new:  func() (closableLimiter, error) { return NewSlidingWindow(perSecond, WithCleanupInterval(0)) },
		},
		{
			name: "nil clock",
			new:  func() (closableLimiter, error) { return NewTokenBucket(perSecond, 1, WithClock(nil)) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l, err := tt.new()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Nil(t, l)
		})
	}
}
