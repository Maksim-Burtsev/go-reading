package backoff_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/backoff"
)

func upper(n int64) int64 { return n - 1 }

func lower(int64) int64 { return 0 }

func TestPolicyDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy backoff.Policy
		retry  int
		want   time.Duration
	}{
		{
			name:   "first retry is bounded by base",
			policy: backoff.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second, Rand: upper},
			retry:  0,
			want:   100*time.Millisecond - 1,
		},
		{
			name:   "ceiling doubles per retry",
			policy: backoff.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second, Rand: upper},
			retry:  3,
			want:   800*time.Millisecond - 1,
		},
		{
			name:   "ceiling is capped at max",
			policy: backoff.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second, Rand: upper},
			retry:  7,
			want:   10*time.Second - 1,
		},
		{
			name:   "huge retry does not overflow",
			policy: backoff.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second, Rand: upper},
			retry:  500,
			want:   10*time.Second - 1,
		},
		{
			name:   "full jitter reaches zero",
			policy: backoff.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second, Rand: lower},
			retry:  4,
			want:   0,
		},
		{
			name:   "zero base never sleeps",
			policy: backoff.Policy{Rand: upper},
			retry:  2,
			want:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.policy.Delay(tt.retry))
		})
	}
}

func TestPolicyDelayStaysWithinCeiling(t *testing.T) {
	t.Parallel()

	policy := backoff.Policy{Base: 50 * time.Millisecond, Max: 2 * time.Second, Rand: rand.Int64N}
	for retry := range 12 {
		ceiling := min(policy.Max, policy.Base<<retry)
		for range 100 {
			d := policy.Delay(retry)
			require.GreaterOrEqual(t, d, time.Duration(0))
			require.Less(t, d, ceiling)
		}
	}
}

func TestSleep(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctx     func(t *testing.T) context.Context
		d       time.Duration
		wantErr error
	}{
		{
			name:    "elapses",
			ctx:     func(*testing.T) context.Context { return context.Background() },
			d:       time.Millisecond,
			wantErr: nil,
		},
		{
			name:    "canceled context",
			ctx:     func(*testing.T) context.Context { return canceled },
			d:       time.Hour,
			wantErr: context.Canceled,
		},
		{
			name: "deadline before delay",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			d:       time.Hour,
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := backoff.Sleep(tt.ctx(t), tt.d)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
