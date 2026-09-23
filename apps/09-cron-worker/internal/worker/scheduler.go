package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrShutdownTimeout is returned by Serve when running jobs did not finish
// within the grace period and had to be cancelled.
var ErrShutdownTimeout = errors.New("running jobs did not finish within the grace period")

// Entry binds a job to its lock name and cron schedule. Specs have six fields,
// the first one being seconds, or are descriptors such as "@every 1m". An entry
// without a Job is disabled. RunOnStart also runs the job once when Serve
// starts, instead of waiting for its first tick.
type Entry struct {
	Name       string
	Spec       string
	Job        Job
	RunOnStart bool
}

// Serve runs entries on their schedules in UTC until ctx is cancelled. It then
// stops scheduling, waits up to grace for running jobs to finish and cancels
// the ones still running.
func (r *Runner) Serve(ctx context.Context, entries []Entry, grace time.Duration) error {
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	logger := cronLogger{logger: r.logger}
	c := cron.New(
		cron.WithSeconds(),
		cron.WithLocation(time.UTC),
		cron.WithLogger(logger),
		cron.WithChain(cron.SkipIfStillRunning(logger), cron.Recover(logger)),
	)
	var startup []Entry
	for _, e := range entries {
		if e.Job == nil {
			r.logger.InfoContext(ctx, "job disabled", "job", e.Name)
			continue
		}
		if _, err := c.AddFunc(e.Spec, func() { r.Run(jobCtx, e.Name, e.Job) }); err != nil {
			return fmt.Errorf("schedule job %s: %w", e.Name, err)
		}
		if e.RunOnStart {
			startup = append(startup, e)
		}
	}

	c.Start()
	r.logger.InfoContext(ctx, "scheduler started", "jobs", len(c.Entries()))
	for _, e := range startup {
		go func() { r.Run(jobCtx, e.Name, e.Job) }()
	}
	<-ctx.Done()
	r.logger.InfoContext(ctx, "scheduler stopping", "grace", grace.String())

	stopped := c.Stop()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-stopped.Done():
		return nil
	case <-timer.C:
		cancelJobs()
		<-stopped.Done()
		return ErrShutdownTimeout
	}
}

type cronLogger struct {
	logger *slog.Logger
}

func (l cronLogger) Info(msg string, keysAndValues ...any) {
	l.logger.Debug(msg, keysAndValues...)
}

func (l cronLogger) Error(err error, msg string, keysAndValues ...any) {
	l.logger.Error(msg, append([]any{"error", err}, keysAndValues...)...)
}
