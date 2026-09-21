# 04-worker-pool

A webhook dispatcher: `POST /deliveries` queues a JSON payload in a bounded in-memory queue, a pool of workers POSTs it to the target with retries, and `GET /deliveries/{id}` reports the outcome.

## Run

```sh
go run ./apps/04-worker-pool/cmd/worker-pool
```

## Where to start

1. `cmd/worker-pool/main.go:run` — wires queue, pool, recorder and HTTP server, then runs the shutdown sequence: stop HTTP, close the queue, drain, cancel.
2. `internal/api/api.go:create` — how a delivery gets in: body limit, validation, `Track`, `Push`, and the 503 when the queue is full.
3. `internal/dispatch/pool.go:Run` — the dispatcher loop: `errgroup` with `SetLimit` pulls tasks off the queue channel and emits one `Result` per task.
4. `internal/dispatch/pool.go:deliver` — the retry loop: one attempt, classify the error, back off with `backoff.Delay` and `backoff.Sleep`.
5. `internal/status/recorder.go:Run` — the single consumer of results, which also evicts old records.

## Data flow

1. `POST /deliveries` reaches `handler.create`, which decodes the body through `http.MaxBytesReader`, validates the URL and payload, and assigns an ID with `crypto/rand.Text`.
2. The recorder `Track`s the delivery as `queued`, then `Queue.Push` does a non-blocking send into a buffered channel; if it is full, the record is dropped and the client gets 503 with `Retry-After`.
3. `Pool.Run` ranges over `Queue.Tasks()`; `g.Go` blocks while `Workers` deliveries are in flight, so pending tasks wait in the channel buffer.
4. `deliver` calls `attempt`, which POSTs with a per-attempt timeout: 2xx is delivered, other non-retryable statuses are wrapped in `ErrPermanent`, and network errors, 429 and 5xx are retried after a jittered backoff, up to `MaxAttempts`.
5. Every task produces exactly one `Result` on `Pool.Results()`; `Recorder.Run` applies it to the record that `GET /deliveries/{id}` returns.
6. On SIGTERM, `serve` shuts the HTTP server down, `run` closes the queue and waits for the pool; when `DRAIN_TIMEOUT` expires, `abort` cancels `workCtx`, in-flight requests stop and the remaining tasks fail as `canceled` without being sent.
7. `Run` closes the results channel after `g.Wait()`, the recorder loop ends, and `run` returns.

## Go specifics here

1. `internal/dispatch/pool.go:63` — a closure started inside a `for range` loop

   <details><summary>Explanation</summary>

   Since Go 1.22 each iteration of `for task := range tasks` gets a fresh `task` variable, so every goroutine captures its own task. In Python a closure created in a loop would see only the last value of the loop variable. `g.Go` also blocks here once `SetLimit` goroutines are running, so this loop is where backpressure happens: it does not receive the next task until the current one has a worker slot.

   </details>

2. `internal/dispatch/queue.go:20` — a read lock around a channel send

   <details><summary>Explanation</summary>

   Sending on a closed channel panics, so `Push` and `Close` must not overlap. `Push` never blocks (the `select` has a `default`), so many producers can safely hold the shared read lock at once. `Close` takes the exclusive write lock, which guarantees that no send is in progress when `close(q.tasks)` runs.

   </details>

3. `cmd/worker-pool/main.go:119` — `context.WithoutCancel`

   <details><summary>Explanation</summary>

   `ctx` is cancelled by SIGTERM. Deliveries must keep running while the queue drains, so `workCtx` keeps `ctx`'s values but not its cancellation, and gets its own `abort` function. `context.AfterFunc(drainCtx, abort)` on line 147 calls `abort` when the drain timeout expires. Python has no built-in equivalent; cancellation in Go is an explicit value passed down the call chain, not an exception raised into a task.

   </details>

4. `internal/status/recorder.go:107` — writing the record back into the map

   <details><summary>Explanation</summary>

   `map[string]Record` stores struct values, not references. `rec, ok := r.records[id]` returns a copy, and `r.records[id].Status = x` does not compile because map elements are not addressable. The code reads, modifies the copy and stores it again, all under the same lock.

   </details>

5. `internal/dispatch/pool.go:134` — two `%w` verbs in one `fmt.Errorf`

   <details><summary>Explanation</summary>

   Since Go 1.20 an error can wrap several errors. The result matches `errors.Is(err, ErrPermanent)`, which `deliver` uses to stop retrying, and `errors.As(err, &statusErr)`, which gives the HTTP status code. Where Python would use a hierarchy of exception classes, Go classifies errors through sentinel values and typed errors in the wrap chain.

   </details>

## Questions

1. All workers are busy and the queue still has room. What does a `POST /deliveries` return, and where does the task wait? What changes once the queue is full?

   <details><summary>Answer</summary>

   It returns 202 and the task waits in the buffered channel inside `Queue`. At most one more task sits outside the buffer: the one the dispatcher loop took before it blocked in `g.Go`. Once the buffer is full, `Push` returns `ErrQueueFull`, the handler calls `Forget` and responds 503 with `Retry-After: 1`.

   </details>

2. Why does `Pool.Run` use a plain `errgroup.Group` and not `errgroup.WithContext`?

   <details><summary>Answer</summary>

   With `WithContext`, the first goroutine that returns an error cancels the shared context, which would abort every other delivery. One failing target must not affect the others. Here the group is used for its concurrency limit and `Wait`; the returned error only reports that cancellation cut the drain short.

   </details>

3. What would change on SIGTERM if `run` passed `ctx` to `pool.Run` instead of `workCtx`?

   <details><summary>Answer</summary>

   The signal would cancel all deliveries immediately: in-flight requests would be aborted and every queued task would fail as `canceled` without an attempt. `DRAIN_TIMEOUT` would have no effect.

   </details>

4. A target always answers 503. With the defaults (`MAX_ATTEMPTS=5`, `BACKOFF_BASE=500ms`, `BACKOFF_MAX=30s`), what is the upper bound on total backoff sleep, and what does `GET /deliveries/{id}` show at the end?

   <details><summary>Answer</summary>

   Five attempts mean four sleeps, with ceilings of 500ms, 1s, 2s and 4s, so less than 7.5s in total (plus up to 10s `ATTEMPT_TIMEOUT` per attempt). The record has `status: failed`, `attempts: 5` and `error: "retries exhausted after 5 attempts: target responded 503 Service Unavailable"`.

   </details>

5. Why does `create` call `Track` before `Push` and not after it?

   <details><summary>Answer</summary>

   Once the task is pushed, a worker can finish it and the recorder can apply its `Result` before `Track` runs. `apply` ignores unknown IDs, and a later `Track` would then store the delivery as `queued` for good; queued records are never evicted. Tracking first and calling `Forget` when `Push` fails avoids that race.

   </details>
