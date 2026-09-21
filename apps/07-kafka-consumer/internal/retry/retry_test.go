package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

func TestPolicyDo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		failures   int
		sleepErr   error
		wantCalls  int
		wantSleeps []time.Duration
		wantErrIs  []error
		wantNotIs  []error
	}{
		{
			name:      "first attempt succeeds",
			wantCalls: 1,
		},
		{
			name:       "succeeds on last attempt",
			failures:   2,
			wantCalls:  3,
			wantSleeps: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
		},
		{
			name:       "attempts exhausted",
			failures:   5,
			wantCalls:  3,
			wantSleeps: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
			wantErrIs:  []error{ErrExhausted, errBoom},
		},
		{
			name:       "context canceled while waiting",
			failures:   5,
			sleepErr:   context.Canceled,
			wantCalls:  1,
			wantSleeps: []time.Duration{100 * time.Millisecond},
			wantErrIs:  []error{context.Canceled, errBoom},
			wantNotIs:  []error{ErrExhausted},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sleeps []time.Duration
			p := New(3, 100*time.Millisecond, time.Second)
			p.jitter = func(ceiling time.Duration) time.Duration { return ceiling }
			p.sleep = func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return tt.sleepErr
			}

			var attempts []int
			err := p.Do(t.Context(), func(_ context.Context, attempt int) error {
				attempts = append(attempts, attempt)
				if len(attempts) <= tt.failures {
					return errBoom
				}
				return nil
			})

			require.Len(t, attempts, tt.wantCalls)
			require.Equal(t, 1, attempts[0])
			require.Equal(t, tt.wantSleeps, sleeps)
			if len(tt.wantErrIs) == 0 {
				require.NoError(t, err)
			}
			for _, target := range tt.wantErrIs {
				require.ErrorIs(t, err, target)
			}
			for _, target := range tt.wantNotIs {
				require.NotErrorIs(t, err, target)
			}
		})
	}
}

func TestPolicyBackoffCeiling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		base  time.Duration
		max   time.Duration
		retry int
		want  time.Duration
	}{
		{name: "first retry waits base", base: 100 * time.Millisecond, max: time.Second, retry: 1, want: 100 * time.Millisecond},
		{name: "doubles each retry", base: 100 * time.Millisecond, max: time.Second, retry: 4, want: 800 * time.Millisecond},
		{name: "capped at max", base: 100 * time.Millisecond, max: time.Second, retry: 5, want: time.Second},
		{name: "no overflow on large retries", base: 100 * time.Millisecond, max: time.Second, retry: 200, want: time.Second},
		{name: "base above max", base: time.Minute, max: time.Second, retry: 1, want: time.Second},
		{name: "zero delays", retry: 3, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := New(3, tt.base, tt.max)
			p.jitter = func(ceiling time.Duration) time.Duration { return ceiling }
			require.Equal(t, tt.want, p.backoff(tt.retry))
		})
	}
}

func TestPolicyBackoffJitterBounds(t *testing.T) {
	t.Parallel()

	p := New(3, 100*time.Millisecond, time.Second)
	for range 1000 {
		d := p.backoff(3)
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.Less(t, d, 400*time.Millisecond)
	}
}

func TestSleepHonorsContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, sleep(ctx, time.Hour), context.Canceled)
	require.NoError(t, sleep(t.Context(), time.Millisecond))
}
