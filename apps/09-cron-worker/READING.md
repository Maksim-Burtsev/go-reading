# 09-cron-worker

A cron scheduler running three Postgres maintenance jobs, each under an advisory lock so that only one instance runs a job at a time, with job duration metrics on `/metrics`.

## Run

```sh
docker compose -p gr-09 -f apps/09-cron-worker/docker-compose.yml up -d --wait && go run ./apps/09-cron-worker/cmd/cron-worker   # integration tests: go test -race -tags integration ./apps/09-cron-worker/...
```

## Where to start

1. `cmd/cron-worker/main.go:run` — the wiring: config, pool, migrations, the three jobs, the HTTP server and how the process lives and dies.
2. `internal/worker/scheduler.go:Serve` — how cron is configured and what happens to running jobs on shutdown.
3. `internal/worker/runner.go:Run` — one job run: lock, run, unlock, metrics, log.
4. `internal/pglock/pglock.go:TryLock` — the advisory lock and why it pins a connection.
5. `internal/jobs/jobs.go:RollupEvents.Run` — a typical job: time from the clock, cutoffs, one SQL statement.

## Data flow

1. `run` parses env into `config`, opens a `pgxpool.Pool` and applies the embedded goose migrations, serialized across instances by goose's own advisory lock.
2. It builds a `worker.Runner` from a `pglock.Locker`, the system clock and a Prometheus registry, plus three `worker.Entry` values (name, cron spec, job).
3. An errgroup runs the HTTP server (`/metrics`, `/healthz`) and `Runner.Serve`; a signal cancels `ctx`, which stops both.
4. cron fires an entry on its six-field UTC spec; the call goes through `cron.Recover` and `cron.SkipIfStillRunning`, then into `Runner.Run(jobCtx, name, job)`.
5. `Runner.Run` asks the locker for the lock called `name`: `Key` hashes the name, and `pg_try_advisory_lock` runs on a connection checked out for the whole run.
6. Lock held elsewhere: the skip counter goes up and the run ends. Lock taken: `job.Run` computes its cutoffs from the clock and executes its SQL through the pool.
7. The lock is released on a context that survives shutdown, the connection goes back to the pool (or is closed on error), and the duration is observed under `job` and `outcome`.
8. On SIGTERM, `Serve` stops cron, waits up to `SHUTDOWN_TIMEOUT` for running jobs, cancels them if needed; the HTTP server shuts down and the pool closes.

## Go specifics here

1. `internal/worker/scheduler.go:40` — closure in the loop body
   <details><summary>Explanation</summary>

   The closure captures `e`, the range variable. Since Go 1.22 every iteration gets a fresh `e`, so each cron entry calls its own job. Before 1.22 there was one `e` for the whole loop and every closure would have run the last entry — the classic bug that forced `e := e` lines in older code. The same late-binding trap exists in Python lambdas created in a loop; here the language fixed it.
   </details>

2. `internal/worker/scheduler.go:29` — `context.WithoutCancel`
   <details><summary>Explanation</summary>

   Contexts form a tree: cancelling a parent cancels every child. `WithoutCancel(ctx)` keeps the parent's values but cuts the cancellation link, and `WithCancel` on top of it gives `Serve` its own switch. So SIGTERM cancels `ctx` but not the running jobs; they get the grace period and are cancelled explicitly only when it runs out. `Runner.Run` uses the same trick so that the unlock query still runs after the job's context was cancelled.
   </details>

3. `internal/pglock/pglock.go:36` — `& math.MaxInt64`
   <details><summary>Explanation</summary>

   Postgres advisory keys are `bigint`, FNV-64 yields a `uint64`, and Go has no implicit numeric conversions. `int64(u)` would compile and silently wrap values above `MaxInt64` into negatives; gosec flags that as a possible overflow (G115). Masking the sign bit first makes the conversion provably lossless at the price of one bit of hash. `hash/maphash` would be shorter but is seeded randomly per process, and the key must be the same in every instance and every release — `TestKey` pins it.
   </details>

4. `internal/pglock/pglock.go:59` — returning a func value
   <details><summary>Explanation</summary>

   `TryLock` returns an `unlock` closure instead of a lock object. The closure captures `conn` and `key`, so the pooled connection stays checked out after `TryLock` returns and only the caller who got the lock can release it. This is the same shape as `context.WithCancel` returning a cancel func. The result list is named (`unlock`, `acquired`, `err`) for documentation, and `acquired` is scanned into directly.
   </details>

5. `internal/migrations/migrations.go:30` — assignment inside `defer`
   <details><summary>Explanation</summary>

   `Up` has a named result `err`. A deferred closure runs after the `return` statement has set `err` but before the caller sees it, so it can still change the returned value: here it joins the `db.Close()` error into whatever `Up` was returning. A bare `defer db.Close()` would silently drop that error. It plays the role of a `finally` block that can amend the result.
   </details>

## Questions

1. Why does `TryLock` keep a `*pgxpool.Conn` checked out until unlock instead of calling `pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)")`?
   <details><summary>Answer</summary>

   A session-level advisory lock belongs to the backend session (the connection) that took it. `pool.QueryRow` borrows some connection and returns it right away, so the lock would stay on an idle pooled connection. The later unlock could land on a different connection and return false, leaking the lock. Worse, session locks are re-entrant: another run in the same instance could get that very connection and "acquire" a lock it already holds. `TestLockerExclusion/same_job_from_the_same_instance` fails if you make that change.
   </details>

2. If `pg_advisory_unlock` fails, `unlock` calls `discard` instead of `conn.Release()`. What would go wrong with `Release()`?
   <details><summary>Answer</summary>

   After a failed unlock the code cannot know whether the session still holds the lock. Returning the connection to the pool could park a live session that holds the job's lock. Every instance would then skip that job until the pool happens to close that connection, which by default takes up to 30 minutes idle or one hour of lifetime. `Hijack` takes the connection out of the pool and `Close` ends the session, and Postgres releases all advisory locks held by a session when it ends.
   </details>

3. Two instances fire `rollup-daily-events` at 00:15:00 and the job takes 3 ms. Is the second instance guaranteed to skip it?
   <details><summary>Answer</summary>

   No. The lock prevents concurrent runs, not repeated ones. If the first instance finishes before the second calls `pg_try_advisory_lock`, both run one after the other. That is acceptable only because every job is idempotent: the rollup upserts on `(day, event_type)`, and purge and expire select by predicate, so a second pass finds nothing to do. At-most-once per period would need a durable record of completed runs, for example a table keyed by job and period.
   </details>

4. SIGTERM arrives while `purge-expired-sessions` is running with the default `SHUTDOWN_TIMEOUT=30s`, and the job needs another 40 s. Trace what happens and the exit code.
   <details><summary>Answer</summary>

   `ctx` is cancelled, so the errgroup's `gctx` is too. The HTTP shutdown goroutine starts `srv.Shutdown`. `Serve` calls `c.Stop()`, which stops new ticks and returns a context that is done when running jobs return. The job keeps running because `jobCtx` is detached from `ctx`. After 30 s the timer fires, `cancelJobs()` cancels `jobCtx`, pgx cancels the in-flight `DELETE`, and the job returns an error. `Runner.Run` still unlocks, on its own `WithoutCancel` context, and records a `failure`. `Serve` returns `ErrShutdownTimeout`, `g.Wait` returns it, `run` returns it and `main` exits with status 1. Batches already committed stay deleted.
   </details>

5. How does `TestRunnerRun` assert a 1.5 s duration without sleeping, and why is `Clock` an interface rather than a call to `time.Now()`?
   <details><summary>Answer</summary>

   `Runner` reads time only through `Clock`, once before taking the lock and once after unlocking. The test passes a `fakeClock` and the fake job advances it by `jobTakes`, so elapsed time is exactly 1.5 s. The histogram's sample count and sum are then read back from the test's own `prometheus.Registry` via `Gather`. The same interface lets `jobs_test.go` pin "now" with `fixedClock` and assert the exact cutoffs passed to SQL, including the UTC day boundaries when the clock is in another time zone. Go has no monkeypatching like `freezegun`, so the time source is injected instead.
   </details>
