// Package worker runs scheduled jobs under a distributed lock and records how
// each run went.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"

	unlockTimeout = 5 * time.Second
)

// Job is a unit of scheduled work.
type Job interface {
	Run(ctx context.Context) error
}

// Locker grants exclusive ownership of a named lock without waiting for it.
// When the lock is held elsewhere, acquired is false and err is nil. The
// returned unlock function must be called exactly once after a successful
// acquisition.
type Locker interface {
	TryLock(ctx context.Context, name string) (unlock func(context.Context) error, acquired bool, err error)
}

// Clock reports the current time.
type Clock interface {
	Now() time.Time
}

// Runner executes jobs while holding their lock and records the outcome of
// every run in logs and metrics.
type Runner struct {
	logger   *slog.Logger
	locker   Locker
	clock    Clock
	duration *prometheus.HistogramVec
	skipped  *prometheus.CounterVec
}

// NewRunner returns a Runner whose metrics are registered with reg.
func NewRunner(logger *slog.Logger, locker Locker, clock Clock, reg prometheus.Registerer) (*Runner, error) {
	r := &Runner{
		logger: logger,
		locker: locker,
		clock:  clock,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cron_worker",
			Name:      "job_duration_seconds",
			Help:      "Duration of job runs that acquired or failed to acquire the job lock.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300, 900},
		}, []string{"job", "outcome"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cron_worker",
			Name:      "job_skipped_total",
			Help:      "Job runs skipped because another instance held the job lock.",
		}, []string{"job"}),
	}
	for _, c := range []prometheus.Collector{r.duration, r.skipped} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register runner metrics: %w", err)
		}
	}
	return r, nil
}

// Run executes job under the lock called name. The run is skipped when the
// lock is held by another instance.
func (r *Runner) Run(ctx context.Context, name string, job Job) {
	logger := r.logger.With("job", name)
	start := r.clock.Now()

	unlock, acquired, err := r.locker.TryLock(ctx, name)
	if err != nil {
		r.finish(ctx, logger, name, start, fmt.Errorf("acquire job lock: %w", err))
		return
	}
	if !acquired {
		r.skipped.WithLabelValues(name).Inc()
		logger.InfoContext(ctx, "job skipped, lock held by another instance")
		return
	}

	err = job.Run(ctx)

	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	if uerr := unlock(unlockCtx); uerr != nil {
		err = errors.Join(err, fmt.Errorf("release job lock: %w", uerr))
	}

	r.finish(ctx, logger, name, start, err)
}

func (r *Runner) finish(ctx context.Context, logger *slog.Logger, name string, start time.Time, err error) {
	elapsed := r.clock.Now().Sub(start)
	outcome := outcomeSuccess
	if err != nil {
		outcome = outcomeFailure
	}
	r.duration.WithLabelValues(name, outcome).Observe(elapsed.Seconds())

	if err != nil {
		logger.ErrorContext(ctx, "job failed", "duration_ms", elapsed.Milliseconds(), "error", err)
		return
	}
	logger.InfoContext(ctx, "job finished", "duration_ms", elapsed.Milliseconds())
}
