package worker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type fakeLocker struct {
	acquired  bool
	lockErr   error
	unlockErr error
	unlocks   int
}

func (l *fakeLocker) TryLock(context.Context, string) (func(context.Context) error, bool, error) {
	if l.lockErr != nil || !l.acquired {
		return nil, false, l.lockErr
	}
	return func(context.Context) error {
		l.unlocks++
		return l.unlockErr
	}, true, nil
}

type jobFunc func(ctx context.Context) error

func (f jobFunc) Run(ctx context.Context) error { return f(ctx) }

func TestRunnerRun(t *testing.T) {
	t.Parallel()

	errJob := errors.New("job failed")
	errDB := errors.New("connection refused")

	tests := []struct {
		name        string
		locker      fakeLocker
		jobTakes    time.Duration
		jobErr      error
		wantRuns    int
		wantUnlocks int
		wantOutcome string
		wantSeconds float64
		wantSkipped float64
	}{
		{
			name:        "success",
			locker:      fakeLocker{acquired: true},
			jobTakes:    1500 * time.Millisecond,
			wantRuns:    1,
			wantUnlocks: 1,
			wantOutcome: outcomeSuccess,
			wantSeconds: 1.5,
		},
		{
			name:        "job error",
			locker:      fakeLocker{acquired: true},
			jobTakes:    250 * time.Millisecond,
			jobErr:      errJob,
			wantRuns:    1,
			wantUnlocks: 1,
			wantOutcome: outcomeFailure,
			wantSeconds: 0.25,
		},
		{
			name:        "unlock error",
			locker:      fakeLocker{acquired: true, unlockErr: errDB},
			jobTakes:    time.Second,
			wantRuns:    1,
			wantUnlocks: 1,
			wantOutcome: outcomeFailure,
			wantSeconds: 1,
		},
		{
			name:        "lock held elsewhere",
			locker:      fakeLocker{acquired: false},
			wantSkipped: 1,
		},
		{
			name:        "lock error",
			locker:      fakeLocker{lockErr: errDB},
			wantOutcome: outcomeFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := &fakeClock{now: time.Date(2026, 9, 21, 3, 0, 0, 0, time.UTC)}
			reg := prometheus.NewRegistry()
			runner, err := NewRunner(slog.New(slog.DiscardHandler), &tt.locker, clock, reg)
			require.NoError(t, err)

			runs := 0
			runner.Run(t.Context(), "purge", jobFunc(func(context.Context) error {
				runs++
				clock.Advance(tt.jobTakes)
				return tt.jobErr
			}))

			require.Equal(t, tt.wantRuns, runs)
			require.Equal(t, tt.wantUnlocks, tt.locker.unlocks)
			require.Equal(t, tt.wantSkipped, counterValue(t, reg, "cron_worker_job_skipped_total", "purge"))

			histograms := durationHistograms(t, reg)
			if tt.wantOutcome == "" {
				require.Empty(t, histograms)
				return
			}
			require.Len(t, histograms, 1)
			h, ok := histograms[tt.wantOutcome]
			require.True(t, ok, "no histogram for outcome %q", tt.wantOutcome)
			require.Equal(t, uint64(1), h.GetSampleCount())
			require.InDelta(t, tt.wantSeconds, h.GetSampleSum(), 1e-9)
		})
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name, job string) float64 {
	t.Helper()
	for _, m := range gather(t, reg, name) {
		if label(m, "job") == job {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

func durationHistograms(t *testing.T, reg *prometheus.Registry) map[string]*dto.Histogram {
	t.Helper()
	out := make(map[string]*dto.Histogram)
	for _, m := range gather(t, reg, "cron_worker_job_duration_seconds") {
		out[label(m, "outcome")] = m.GetHistogram()
	}
	return out
}

func gather(t *testing.T, reg *prometheus.Registry, name string) []*dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()
		}
	}
	return nil
}

func label(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}
