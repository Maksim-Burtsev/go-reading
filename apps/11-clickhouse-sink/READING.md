# 11 · clickhouse-sink

An HTTP service that takes analytics events as JSON arrays on `POST /ingest`, answers 202 as soon as they are
held in a bounded in-memory buffer, and writes them to ClickHouse over the native protocol (clickhouse-go) in
batches of up to 10 000 events or once a second. Events must be at most 30 days old; a full buffer answers 429
instead of making the client wait, a batch ClickHouse keeps refusing is dropped and counted in Prometheus
metrics, and on SIGTERM the service stops taking requests and writes out what it still holds.

## Run it

```sh
docker compose -f apps/11-clickhouse-sink/docker-compose.yml up -d --wait
go run ./apps/11-clickhouse-sink/cmd/clickhouse-sink
```

In a second terminal:

```sh
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -si localhost:8080/ingest -d '[{"event_id":"0b5e4a1c-2f4e-4c55-9d2b-4f6a3c1e8a01","event_type":"page_view","user_id":"u-1","ts":"'"$now"'","properties":{"path":"/"}}]'
curl -s localhost:8080/metrics | grep '^sink_'
docker compose -f apps/11-clickhouse-sink/docker-compose.yml exec clickhouse \
  clickhouse-client --user sink --password sink -d sink --query 'SELECT count() FROM events'
```

Stop ClickHouse with `docker compose -f apps/11-clickhouse-sink/docker-compose.yml stop clickhouse` and post
again: the request still gets 202, `/health` turns 503, and a second or so later `sink_events_dropped_total`
grows. `docker compose -f apps/11-clickhouse-sink/docker-compose.yml down -v` removes the stack.

## Questions

Answer from the code first, then open the answer.

1. SIGTERM arrives while requests are in flight and a flush is retrying against an unreachable ClickHouse. In
   what order do things stop, and what bounds how long the process takes to exit?

   <details><summary>Answer</summary>

   `serve` first calls `srv.Shutdown`, which closes the listener and waits for in-flight handlers, because they
   may still call `Enqueue`. The batcher runs on a context built from `context.WithoutCancel(ctx)`, so the
   signal alone does not stop it; `serve` cancels it with `stopBatcher` once `Shutdown` returns. `Run` detaches
   its flushes the same way, so its cancellation does not cut an insert off mid-send, and `context.AfterFunc`
   arms `time.AfterFunc(DrainTimeout, cancel)` the moment it is cancelled: the flush in progress and the drain
   share that one budget, and what is still unwritten when it runs out is dropped and counted. Exit takes at
   most the HTTP shutdown plus the drain, `SHUTDOWN_TIMEOUT` each by default. Rule: stop producers before their
   consumer, and when you shield work with `WithoutCancel`, give it back a deadline counted from the cancellation.

   </details>

2. `NewHandler` takes a `now func() time.Time`, and `run` passes `time.Now`. Why not call `time.Now()` inside
   the handler?

   <details><summary>Answer</summary>

   `Validate` accepts a `ts` only from the 30 days before `now` up to a day after it; older or stranger
   timestamps would be stored wrong, or spread one INSERT over more partitions than ClickHouse accepts. That
   window moves with the clock, so a handler test posting a fixed timestamp would pass today and fail a month
   later if the handler read the wall clock. Taking the clock as a parameter lets the tests pin it to a fixed
   instant, and a plain `func() time.Time` is the smallest seam that does it: no interface or clock type needed.
   Rule: inject the clock wherever behavior depends on the current time, the same way you inject any other
   input that changes by itself.

   </details>

3. A client sends two JSON arrays back to back in one body, `[...][...]`. What does it get back, and what would
   it get without the second `dec.Decode` call in `decodeEvents`?

   <details><summary>Answer</summary>

   `json.Decoder` reads one JSON value from a stream and stops; what follows stays unread and is not an error. The
   second `Decode` has to end in `io.EOF`, so anything after the array becomes `errTrailingData` and a 400. Without
   it the handler would enqueue the first array, answer 202 and silently drop the second. `json.Unmarshal` would
   reject the same body but needs all of it in memory first. The decoder reads through `http.MaxBytesReader`
   instead: past 4 MiB the read fails with `*http.MaxBytesError`, which the handler finds with `errors.As` and
   answers with 413. Rule: `json.Decoder` is a stream reader, so when a body must hold exactly one value, check
   for `io.EOF` after it.

   </details>

4. ClickHouse slows down and the buffer fills. What does a client get, and why does `Enqueue` check free space
   under a mutex instead of the usual `select { case b.events <- e: default: return ErrBufferFull }`?

   <details><summary>Answer</summary>

   While `Run` sits in a slow flush it does not receive, so the channel fills; `Enqueue` finds fewer free slots
   than the request has events and returns `ErrBufferFull`, which becomes 429 with `Retry-After: 1`, and nothing
   from the request is kept. A `select` with `default` decides one value at a time: a 50-event request could get
   30 events in, answer 429, and the client's retry would store those 30 twice. Holding `b.mu` across the check
   and the sends makes a request all-or-nothing, and the sends cannot block because the only other party, `Run`,
   only takes values out. Rule: `select` with `default` admits one item without blocking; admitting n items at
   once needs the capacity check and the sends under one lock, and a full queue should fail fast with a
   retryable status rather than park the request goroutine.

   </details>

5. After every flush `Run` starts the next batch with `make([]event.Event, 0, b.cfg.BatchSize)` rather than
   reusing `batch[:0]`. What would reusing it depend on?

   <details><summary>Answer</summary>

   `batch[:0]` keeps the same backing array, so the next `append` calls overwrite the elements the previous flush
   was handed. That is safe only if nothing holds the old slice once `InsertEvents` returns. The ClickHouse store
   copies every row into the driver's column buffers before `Send`, so reuse would work with it today, but an
   `Inserter` that kept the slice, such as an asynchronous writer or a test fake that records calls, would see
   its old batch change under it; the batcher tests' fake clones what it records for that reason. A fresh array
   per batch costs one allocation per flush and removes the question. Rule: passing a slice shares its memory,
   so reslice to `s[:0]` only when nobody else can still hold it.

   </details>

6. A client got 202 for an event. In which ways can that event end up in ClickHouse zero times, or twice?

   <details><summary>Answer</summary>

   Zero times: the batch fails `FLUSH_MAX_ATTEMPTS` times and `flush` drops it, or the process dies (OOM kill, a
   SIGKILL before the drain finishes) while the event is still in memory. Twice: ClickHouse committed an INSERT
   whose acknowledgement never arrived (a read timeout, a reset connection) and `insertWithRetry` sent the batch
   again, or the client timed out before the 202 and posted again. The table is a plain `MergeTree` ordered by
   `(event_type, ts)`, so nothing collapses the copies; `event_id` makes them detectable with
   `uniqExact(event_id)`, and a `ReplacingMergeTree` with `event_id` in its sorting key would merge them away.
   Rule: an acknowledgement from an in-memory buffer promises neither at-most-once nor at-least-once, and a
   retried write is safe only when it is idempotent, which is what a client-supplied ID makes possible.

   </details>

7. Which metric would you alert on for lost events, and why is `sink_buffer_length` a `GaugeFunc` rather than a
   gauge that `Enqueue` and `Run` keep up to date?

   <details><summary>Answer</summary>

   `sink_events_dropped_total` counts events discarded after the last failed attempt; alert on its `increase`,
   which works because a counter only goes up and `rate`/`increase` handle restarts. `sink_flush_errors_total`
   counts failed attempts, which grow without any loss whenever a retry succeeds. `sink_buffer_length` runs
   `len(b.events)` at scrape time, so it cannot drift from the channel and costs nothing per request, but it only
   sees the channel: the batch `Run` is flushing, up to `BATCH_SIZE` more events, is invisible, and with
   `BUFFER_SIZE=4` and `BATCH_SIZE=2` six events can be in memory while it reads 4. Rule: count what happens
   with counters and query them with `rate` or `increase`; read current state at scrape time when you can compute
   it, and check what a gauge covers before reading it as "work in flight".

   </details>

8. The ClickHouse store's `InsertEvents` declares its result as `(err error)` and closes the batch in a deferred
   closure. What does the caller see when `Append` fails and `Close` fails too, and what would a plain
   `defer batch.Close()` lose?

   <details><summary>Answer</summary>

   A deferred function runs after the `return` statement has set the results and before the caller receives them,
   and a closure that refers to the named result `err` can still change it. Here it joins the `Close` error into
   whatever was being returned, so the caller sees the append failure and the close failure together; a plain
   `defer batch.Close()` would throw the second one away. After a successful `Send` the driver's `Close` returns
   nil, so the join matters on the early returns, where `Close` also hands the connection back to the pool. The
   `if err := ...` statements inside declare new, shadowing variables that reach the named result only through an
   explicit `return`. Rule: when a deferred cleanup can fail, name the result and assign to it from a deferred
   closure, or the cleanup error is lost.

   </details>
