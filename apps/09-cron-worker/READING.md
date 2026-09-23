# 09 · cron-worker

A background worker that runs three Postgres maintenance jobs on cron schedules in UTC: it purges expired sessions every five minutes, rolls yesterday's events up into daily stats at 00:15, and expires orders left pending for 30 minutes, checking every minute. Each run takes a Postgres advisory lock named after its job, so several instances can run side by side, and run durations are exported on `/metrics`. It is built on robfig/cron, pgx and the Prometheus client.

## Run it

```sh
docker compose -f apps/09-cron-worker/docker-compose.yml up -d --wait
EXPIRE_ORDERS_SCHEDULE='*/5 * * * * *' go run ./apps/09-cron-worker/cmd/cron-worker

# in a second terminal
curl -s localhost:9090/metrics
curl -s localhost:9090/healthz
```

The schedule override makes the order-expiry job fire every five seconds, so runs show up in the JSON log and on `/metrics` right away. If port 5432 is taken, start compose with `POSTGRES_PORT=5409` and the worker with `DATABASE_URL=postgres://cron:cron@localhost:5409/cron?sslmode=disable`.

## Questions

Answer from the code first, then open the answer.

1. `ROLLUP_EVENTS_SCHEDULE` is set to the five-field spec `15 0 * * *`. Trace what happens to the HTTP server and to the exit status. What would change if the shutdown goroutine in `run` waited on `ctx` instead of `gctx`?

   <details><summary>Answer</summary>

   `cron.WithSeconds()` makes the parser require six fields, so `Runner.Serve` fails in `AddFunc` before cron starts and returns the error. `errgroup.WithContext` cancels `gctx` as soon as any member returns an error: the shutdown goroutine wakes and calls `srv.Shutdown`, `srv.Serve` returns `http.ErrServerClosed`, which its member turns into nil, and `g.Wait` returns the first error, so `main` exits with status 1. If that goroutine waited on the signal `ctx`, nothing would stop the HTTP server: `g.Wait` would block until SIGTERM, leaving a process with a green `/healthz` and no scheduler. Rule: every member of an errgroup must also stop when the group's context is cancelled, or one failure cannot bring the others down.

   </details>

2. Each cron entry is registered with `func() { r.Run(jobCtx, e.Name, e.Job) }` inside `for _, e := range entries`. What would the three schedules run if the module's `go.mod` said `go 1.21`?

   <details><summary>Answer</summary>

   Before Go 1.22 a `range` loop had one `e` for the whole loop, and every closure captured that same variable. The closures run later, when cron fires, and by then `e` holds the last entry, so all three schedules would run `expire-pending-orders` under its lock name: sessions would never be purged and events never rolled up, with no error anywhere. Since 1.22 every iteration gets a fresh variable, and which rule applies is decided by the `go` line of the module, not by the compiler version. Older code guards with `e := e`, and the same trap hit table tests that call `t.Parallel()` inside `t.Run`. Python lambdas created in a loop bind late in exactly this way. Rule: a closure captures variables, not values, so check what a deferred or asynchronous closure will see when it runs.

   </details>

3. SIGTERM arrives while `purge-expired-sessions` is running, `SHUTDOWN_TIMEOUT` is the default 30 s, and the job needs another 40 s. Trace the job, its lock and the exit status.

   <details><summary>Answer</summary>

   `ctx` and `gctx` are cancelled; `Serve` calls `c.Stop()`, which stops new ticks and returns a context that is done once running jobs return. The job keeps going because `jobCtx` is detached from `ctx` with `context.WithoutCancel` and has its own cancel function. After 30 s `cancelJobs()` fires; pgx returns from the `Exec` at once, by default by breaking the connection rather than sending a cancel request, so Postgres may still finish and commit that batch. `Runner.Run` unlocks on a detached context with a five-second timeout, since on `jobCtx` the unlock query would fail, and records a `failure`. `Serve` returns `ErrShutdownTimeout`, `g.Wait` returns it, and the process exits with status 1. Rule: give in-flight work a context that shutdown cancels on its own schedule, and run cleanup on a detached context with a deadline.

   </details>

4. A job panics with a nil map write. What happens to its lock, its duration metric and its next scheduled run, and why is the panic recovered inside `Runner.Run` instead of being left to `cron.Recover`?

   <details><summary>Answer</summary>

   A panic unwinds its goroutine running only deferred calls, so ordinary statements after the panicking call are skipped, and `recover` stops it only when called from a deferred function in that goroutine. `Runner.Run` calls the job through a small function whose deferred `recover` turns the panic into an error, so the rest of `Run` still happens: the lock is released, the run is logged and counted as a `failure`, and the next tick runs the job as usual. Left to `cron.Recover`, the panic would skip the unlock and the connection holding the advisory lock would stay checked out until exit, so every instance would skip the job from then on. `SkipIfStillRunning` returns its token with a plain statement, so it sits outside `cron.Recover`, where no panic reaches it. Rule: recover where the cleanup lives, or defer the cleanup, because a panic skips everything that is not deferred.

   </details>

5. Why does `TryLock` keep a `*pgxpool.Conn` checked out until `unlock` instead of running `SELECT pg_try_advisory_lock($1)` through the pool, and why does a failed unlock hijack and close that connection instead of releasing it?

   <details><summary>Answer</summary>

   A session-level advisory lock belongs to the Postgres session, the connection, that took it. `pool.QueryRow` returns the connection as soon as the row is read, leaving the lock on an idle pooled connection: the unlock may run elsewhere and return false, and since session locks are re-entrant, a later run that gets that connection "acquires" the lock again. The `unlock` closure captures the checked-out connection, pinning the session for the whole run. After a failed unlock the session may still hold the lock; `Release` would park it in the pool for up to 30 minutes idle or an hour of lifetime by default, and every instance would skip the job. `Hijack` plus `Close` ends the session, and Postgres drops its locks. Rule: session state such as advisory locks or `SET` pins you to one connection; never pool a connection in an unknown state.

   </details>

6. Two instances fire `rollup-daily-events` at 00:15:00 and a run takes 3 ms. Is the second instance guaranteed to skip it, and what would go wrong with a job that is not idempotent?

   <details><summary>Answer</summary>

   No. The lock excludes concurrent runs, not repeated ones: if the first run has already unlocked when the second instance calls `pg_try_advisory_lock`, the second takes the lock and runs again, and `SkipIfStillRunning` only guards against overlap inside one process. That is harmless here because each job converges: the rollup overwrites the totals for `(day, event_type)`, and purge and expire act on a predicate that a second pass no longer matches. A job that adds to counters or sends e-mails would do it twice. Once per period needs a durable record written in the same transaction as the work, for example a row keyed by job and period inserted with `ON CONFLICT DO NOTHING`. Rule: a distributed lock serializes work; exactly-once effects come from idempotent writes or a durable dedupe key.

   </details>

7. There are two million expired sessions. Why does `PurgeSessions` loop over `DELETE ... WHERE id IN (SELECT ... LIMIT $2 FOR UPDATE SKIP LOCKED)` instead of issuing one `DELETE ... WHERE expires_at < $1`, and when does the loop stop?

   <details><summary>Answer</summary>

   Each `Exec` on the pool is its own autocommit transaction, so every batch commits separately: row locks are held for one batch at a time, WAL is written in small pieces, and when the job is cancelled at shutdown the finished batches stay done. One big `DELETE` would lock every expired row until it commits and lose all its work if interrupted. `FOR UPDATE SKIP LOCKED` lets the subquery pass over rows another transaction holds locked, such as a request extending that session, instead of waiting; they are left for the next run. The cutoff is read once before the loop, which stops at the first batch that deletes fewer than `batchSize` rows, so it ends even while sessions keep expiring. Rule: change large sets in bounded, separately committed batches, and fix the predicate before the loop starts.

   </details>

8. `TestRunnerRun` asserts a run of exactly 1.5 s without sleeping, and the job tests assert exact SQL cutoffs. How, and why do `worker` and `jobs` each declare their own `Clock` interface?

   <details><summary>Answer</summary>

   `Runner` never calls `time.Now`; it reads its `Clock` before taking the lock and again after unlocking. The test passes a `fakeClock` that the fake job advances by 1.5 s, so the observed duration is exact, and reads the histogram back from its own registry. The jobs read time the same way, so `fixedClock` pins now and the tests assert the exact cutoffs, including UTC day boundaries for a clock in another time zone. Each package declares the one-method interface it needs, and `systemClock` in `main` satisfies both without naming either, because a Go type implements an interface just by having its methods. Go has no freezegun-style patching of `time.Now`, so the clock is injected instead. Rule: inject time like any other dependency, and let each consumer declare the small interface it uses.

   </details>
