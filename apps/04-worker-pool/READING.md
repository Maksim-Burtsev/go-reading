# 04 · worker-pool

An HTTP service that delivers webhooks for its clients. `POST /deliveries` takes a target URL and a
JSON payload and answers 202 with a delivery ID; a pool of goroutines capped with
`errgroup.SetLimit` POSTs the payload to the target, retrying network errors, 429 and 5xx with
jittered exponential backoff, and `GET /deliveries/{id}` reports the outcome. Everything lives in
memory: a full queue answers 503, and on SIGTERM the service stops taking work, drains the queue for
a bounded time and cancels whatever is left.

## Run it

```sh
WORKERS=1 QUEUE_SIZE=1 DRAIN_TIMEOUT=3s go run ./apps/04-worker-pool/cmd/worker-pool
```

In a second terminal:

```sh
curl -i localhost:8080/deliveries -d '{"url":"http://127.0.0.1:1/hook","payload":{"event":"order.paid"}}'
for i in 1 2 3; do curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/deliveries -d '{"url":"http://127.0.0.1:1/hook","payload":{}}'; done
curl -s localhost:8080/deliveries/ID
```

`ID` is the one from the `Location` header. Nothing listens on port 1, so every attempt is refused
and retried: three POSTs get 202 and the fourth 503. Press Ctrl-C in the first terminal while
retries are running to watch the drain time out.

## Questions

Answer from the code first, then open the answer.

1. SIGTERM arrives while deliveries are still retrying. What would change if `run` passed `ctx` to
   `pool.Run` instead of `workCtx`?

   <details><summary>Answer</summary>

   `ctx` comes from `signal.NotifyContext`, so the signal cancels it at once. `workCtx` is built
   with `context.WithoutCancel(ctx)`: it keeps the values of `ctx` but not its cancellation, and has
   its own `abort`, which `context.AfterFunc(drainCtx, abort)` calls only when `DRAIN_TIMEOUT` runs
   out. With `ctx` passed directly, the signal would abort in-flight requests and backoff sleeps
   immediately, every task still in the queue would be reported `canceled` with zero attempts, and
   `DRAIN_TIMEOUT` would have no effect. Rule: give background work a context detached from the
   shutdown signal but bounded by its own deadline, so that "stop accepting" and "stop working" are
   separate steps.

   </details>

2. `run` calls `queue.Close()` while HTTP handlers may still be calling `Push`. What does closing
   the channel tell `Pool.Run`, and why do `Push` and `Close` share an `RWMutex`?

   <details><summary>Answer</summary>

   Closing a channel means "no more values": receivers still get everything already buffered, and
   only then does `for task := range tasks` end, so queued deliveries still go out during the drain.
   Sending on a closed channel panics, so a `close` must never overlap a send. `Push` holds the read
   lock around its `closed` check and its send; many pushes can hold it at once because none of them
   blocks (the `select` has a `default`). `Close` takes the write lock, which waits for in-flight
   pushes and makes every later one see `closed` and return `ErrQueueClosed`. Rule: close a channel
   from the sending side and only once no send can still happen; with many independent senders,
   guard the sends and the close with a lock and a flag.

   </details>

3. All workers are busy and another `POST /deliveries` arrives. Where does the new task wait, and
   when do clients start getting 503 instead of 202?

   <details><summary>Answer</summary>

   `Push` sends inside a `select` with a `default` case, so the handler never blocks: the send
   either succeeds at once or falls through. `Pool.Run` is parked in `g.Go`, because
   `g.SetLimit(p.cfg.Workers)` makes `Go` block until a running goroutine returns; while parked it
   stops receiving, so new tasks pile up in the channel buffer, plus the one task the loop took
   before it blocked. With `WORKERS=1` and `QUEUE_SIZE=1` that is three accepted deliveries; the
   fourth `Push` takes the `default` branch, and the handler forgets the record and answers 503 with
   `Retry-After: 1`. Rule: a bounded buffer plus a non-blocking send turns overload into an
   immediate, explicit rejection instead of blocked handlers or growing memory.

   </details>

4. Why does `create` call `Track` before `Push`, and not after it?

   <details><summary>Answer</summary>

   Once `Push` returns, the task belongs to another goroutine: a worker can deliver it and
   `Recorder.Run` can apply its `Result` before the handler reaches its next line. `apply` ignores
   IDs it does not know, so with `Track` after `Push` a fast result could be dropped, and the late
   `Track` would then store the delivery as `queued` for good; queued records are never evicted.
   Tracking first, and calling `Forget` when `Push` fails, closes that window. Rule: create the
   state that will receive a result before handing the work to another goroutine.

   </details>

5. `Pool.Run` uses a plain `errgroup.Group`. What would `errgroup.WithContext` change here, and
   when would it start to hurt?

   <details><summary>Answer</summary>

   `WithContext` returns a group and a derived context that is canceled when the first function
   passed to `Go` returns a non-nil error or when `Wait` returns. Here the goroutine returns an
   error only for `StatusCanceled`, which happens only after `ctx` is already canceled, so today
   nothing would change. It would hurt the day someone also returns `res.Err` for failed deliveries:
   the first target answering 400 would cancel the group context and abort every other in-flight
   delivery. The plain group says the deliveries are independent and uses errgroup only for its
   limit and `Wait`. Rule: use `WithContext` when one failure should stop the siblings; for
   independent jobs use a plain `Group` and handle each result where it is produced.

   </details>

6. One target answers 400 and another 503. How does `deliver` decide to stop or to retry, and what
   would break if `attempt` built its errors with `%v` instead of `%w`?

   <details><summary>Answer</summary>

   For a 400, `attempt` returns `fmt.Errorf("%w: %w", ErrPermanent, &StatusError{Code: code})`. An
   error made with several `%w` verbs unwraps to a list (Go 1.20), and `errors.Is` and `errors.As`
   search the whole tree, so this one matches both `ErrPermanent` and `*StatusError`. `deliver`
   stops after one attempt when `errors.Is(err, ErrPermanent)`; a 503 comes back as a bare
   `*StatusError` and is retried. With `%v` only the text would survive: `errors.Is` would be false,
   the 400 would be retried `MAX_ATTEMPTS` times, and the record would end as "retries exhausted".
   Rule: classify errors by wrapping sentinels and typed errors with `%w` and testing them with
   `errors.Is` and `errors.As`, never by matching message text.

   </details>

7. Why does `backoff.Sleep` use a timer and a `select` instead of `time.Sleep`, and why is the
   delay drawn at random from [0, ceiling) rather than being exactly `Base*2^retry`?

   <details><summary>Answer</summary>

   `time.Sleep` cannot be interrupted, so a delivery waiting out a 30-second backoff would hold up
   the drain for the whole wait; a `select` on `ctx.Done()` and the timer's channel returns as soon
   as either is ready. The randomness is "full jitter": deliveries that failed together against one
   target would otherwise retry at the same instants and hit it in synchronized waves, and spreading
   each wait uniformly below a ceiling that doubles per retry (up to `Max`) breaks the waves while
   keeping the exponential growth. With the defaults, a delivery that always gets 503 waits less
   than 7.5 s in total across its four sleeps. Rule: every wait in a service should be cancellable,
   and retries against a shared dependency need jitter, not only exponential growth.

   </details>

8. `apply` reads a `Record` from the map, changes it and stores it back. Why can't it assign
   `r.records[res.TaskID].Status` directly, and what would change if the map held `*Record` instead?

   <details><summary>Answer</summary>

   A `map[string]Record` stores struct values, and map elements are not addressable, so assigning to
   a field of `r.records[res.TaskID]` does not compile; `rec, ok := r.records[res.TaskID]` yields a
   copy, which `apply` edits and writes back under the same lock. The copy is also what makes `Get`
   safe: it returns a snapshot, and the handler encodes it to JSON after the lock is released. With
   `map[string]*Record`, `apply` could edit in place, but `Get` would hand the handler a pointer to
   the struct that `apply` later writes to, a data race unless every reader also holds the lock.
   Rule: struct values in maps, slices and channels give copy-on-read snapshots; pointers mean
   shared state that every reader must synchronize.

   </details>
