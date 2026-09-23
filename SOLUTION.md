# 009 · jobs: roll up today's event stats every five minutes

5 defects, 2 decoys.

## 1. An additive rollup that has to run exactly once per window (high)

`apps/09-cron-worker/internal/jobs/jobs.go:114`, in `rollupRecentEventsSQL`, run by
`RollupRecentEvents.Run`; `RunOnStart: true` at `apps/09-cron-worker/cmd/cron-worker/main.go:90`

What goes wrong: the upsert adds each window's counts to the stored row, so today's totals are
right only if every window is applied exactly once. Nothing guarantees that, and the package doc
at `main.go:7` says jobs must tolerate repeated runs.

- Two instances tick at 10:30:00. A takes the lock, adds 10:25–10:30 in a few milliseconds and
  unlocks; B's tick lands 10 ms later by its own clock, finds the lock free and adds the same
  window again. The lock serializes runs; it does not make them happen once.
- `RunOnStart` re-adds the last complete window on every start: an instance that starts at 10:32
  adds 10:25–10:30, which the 10:30 tick already added. Every deploy or crash loop double counts,
  once per instance.
- A window that no run applies (a failed run, every instance down across a boundary) is lost.

The daily rollup overwrites the finished day at 00:15, so the damage lasts exactly as long as the
numbers this feature exists for. Measured against Postgres with three login events between 10:20
and 10:30: after the two ticks `event_count` was 3, a restart at 10:32 made it 5, and a second
instance repeating the 10:30 tick made it 7.

The tell: `+ EXCLUDED.event_count` in an upsert whose window comes from a clock, with no durable
record of which windows were applied, next to a package doc that requires jobs to tolerate
repeated runs, and `RunOnStart` set on that very job.

Fix: make each run converge by recomputing the day so far and overwriting it, which the existing
`rollupEventsSQL` already does:

```go
func (j *RollupRecentEvents) Run(ctx context.Context) error {
	to := j.clock.Now().UTC().Truncate(j.window)
	y, m, d := to.Add(-j.window).Date()
	from := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	tag, err := j.db.Exec(ctx, rollupEventsSQL, pgtype.Date{Time: from, Valid: true}, from, to)
```

At midnight the last window belongs to the previous day, so that run recomputes the whole previous
day. If recomputing the day is too expensive, record applied windows in a ledger table in the same
transaction (`ON CONFLICT DO NOTHING` on the window start) and add only when the insert happened.

Rule: a lock gives at-most-one-at-a-time, not exactly-once; a scheduled job must converge, by
overwriting from the source data or deduplicating on a durable key, never by adding increments.

Found if: names the additive upsert and says a window can be added twice (another instance after
the lock is released, or `RunOnStart` after a restart), or that missed windows are lost.

## 2. Distinct user counts are summed across windows (medium)

`apps/09-cron-worker/internal/jobs/jobs.go:115`, in `rollupRecentEventsSQL`

What goes wrong: `count(DISTINCT user_id)` is distinct within one window. Adding it across windows
counts a user once for every window in which they were active, even with a single instance and no
restarts. A user active all day adds 288 to `unique_users` (one per five-minute window). In the
measurement above, one user active in both windows made `unique_users` 3 for 2 real users after
the two ticks.

The tell: a `count(DISTINCT ...)` result added to a previous `count(DISTINCT ...)` result.

Fix: the same recompute-from-start-of-day statement as in defect 1; a distinct count needs the
rows, not the per-window numbers.

Rule: distinct counts do not add up; combining them needs the underlying sets (or a sketch such as
HyperLogLog), never a sum.

Found if: says `unique_users` is summed across windows, so a user is counted once per window.

## 3. Startup runs escape the scheduler's shutdown (medium)

`apps/09-cron-worker/internal/worker/scheduler.go:59`, in `Serve`

What goes wrong: the startup run is a bare goroutine that `Serve` never waits for and that cron
does not know about. `Serve` promises to wait up to `grace` for running jobs and to cancel only
those still running; neither holds for startup runs. On SIGTERM during a startup run, the context
from `c.Stop()` is done at once (no cron job is running), so `Serve` returns nil, and its
deferred `cancelJobs()` cancels the startup run mid-statement with no grace period, while the
process exits 0 as if it had drained. That is the situation `RunOnStart` exists for: a deploy that
restarts instances. The run also bypasses `SkipIfStillRunning`, so a tick that fires during it
takes a second connection, fails `TryLock` against this instance's own lock, and is counted in
`cron_worker_job_skipped_total` as "lock held by another instance". Measured: with a 10 s grace and
a startup run needing 2 s, `Serve` returned nil after 0.4 ms and the run got `context.Canceled`; an
every-second tick during a 1.5 s startup run was counted as skipped twice.

The tell: a `go` statement in a function whose doc comment promises to drain its jobs, and nothing
(a `WaitGroup`, an errgroup, a channel) that waits for it.

Fix: run the entry's wrapped job, so the cron chain applies, and wait for it together with the
cron jobs:

```go
		id, err := c.AddFunc(e.Spec, func() { r.Run(jobCtx, e.Name, e.Job) })
		if err != nil {
			return fmt.Errorf("schedule job %s: %w", e.Name, err)
		}
		if e.RunOnStart {
			startup = append(startup, id)
		}
	}

	c.Start()
	var startups sync.WaitGroup
	for _, id := range startup {
		startups.Go(c.Entry(id).WrappedJob.Run)
	}
	<-ctx.Done()

	stopped := c.Stop()
	drained := make(chan struct{})
	go func() {
		<-stopped.Done()
		startups.Wait()
		close(drained)
	}()
	// The grace select waits on drained instead of stopped.Done().
```

Rule: every goroutine a component starts belongs to its lifecycle; whatever promises a graceful
stop must be able to wait for it.

Found if: says the startup goroutine is not tracked, so shutdown does not wait for it (cancels it
without grace, or returns nil while it runs), or that it bypasses `SkipIfStillRunning`.

## 4. The disabled job is a typed nil, so it is not disabled (medium)

`apps/09-cron-worker/cmd/cron-worker/main.go:136`, in `newRollupRecent`, with the nil check at
`apps/09-cron-worker/internal/worker/scheduler.go:44`

What goes wrong: with `ROLLUP_RECENT_WINDOW=0`, `newRollupRecent` returns a nil
`*jobs.RollupRecentEvents`. Stored in `Entry.Job`, it becomes an interface value whose dynamic type
is set and whose pointer is nil. An interface is nil only when both are nil, so `e.Job == nil` in
`Serve` is false: the job is scheduled and run on start. `Run` reads `j.clock` through the nil
pointer and panics; `Runner.Run` recovers the panic and logs "job failed" with a stack, counted as a
failure, at startup and every five minutes. The setting documented to turn the job off gives a job
that fails forever, and alerts on job failures fire for it.

The tell: a helper returning a concrete pointer type that may be nil, stored in an interface-typed
field that is later compared with nil. The new scheduler test leaves `Job` unset, an untyped nil, so
it cannot see this.

Fix:

```go
func newRollupRecent(cfg config, db jobs.DB, clock jobs.Clock, logger *slog.Logger) worker.Job {
	if cfg.RollupRecentWindow == 0 {
		return nil
	}
	return jobs.NewRollupRecentEvents(db, clock, logger, cfg.RollupRecentWindow)
}
```

Rule: a nil pointer inside an interface is not a nil interface; a function that can return
"nothing" into an interface should return the interface type.

Found if: says the nil `*RollupRecentEvents` in the `Job` interface is not nil, so the disabled job
still runs (and fails).

## 5. The run-on-start test passes without run-on-start (low)

`apps/09-cron-worker/internal/worker/scheduler_test.go:141`, in `TestRunnerServeRunOnStart`

What goes wrong: the entry's spec `* * * * * *` fires every second and the test waits up to 3 s,
so the first regular tick satisfies it whether `RunOnStart` works or not. With the startup loop
deleted from `Serve`, the test still passed, in 0.24 s.

The tell: an every-second schedule in a test whose subject is running before the first tick.

Fix:

```go
		served <- runner.Serve(ctx, []Entry{{Name: "job", Spec: "0 0 0 1 1 *", Job: job, RunOnStart: true}}, time.Second)
	}()

	select {
	case <-ran:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the job did not run on start")
	}
```

Rule: arrange a test so that the behavior under test is the only way to reach its assertion.

Found if: says the every-second spec lets a regular tick satisfy the test, so it passes without
`RunOnStart`.

## Not defects

### Capturing the loop variable in a goroutine

`apps/09-cron-worker/internal/worker/scheduler.go:59`: `go func() { r.Run(jobCtx, e.Name, e.Job) }()`
inside `for _, e := range startup`. Before Go 1.22 every goroutine could see the last `e`; since
1.22 each iteration has its own `e`, and `go.mod` says 1.27. What is wrong on that line is defect 3,
not the capture.

### Appending to a nil slice

`apps/09-cron-worker/internal/worker/scheduler.go:42`: `var startup []Entry` is nil. `append`
allocates on first use and `range` over a nil slice runs zero times, so no `make` and no nil check
are needed.

## Also acceptable

- `ROLLUP_RECENT_SCHEDULE` and `ROLLUP_RECENT_WINDOW` are set independently, and nothing checks that
  the schedule fires once per window; any other combination adds windows twice or skips them.
- Events inserted after their window was added are not counted until the daily rollup at 00:15.
- Entries without a job are skipped before their spec is parsed, so a typo in a disabled job's
  schedule surfaces only when the job is turned on.
