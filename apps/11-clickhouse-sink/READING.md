# 11-clickhouse-sink

An HTTP ingestion service that buffers analytics events in memory and writes them to ClickHouse in batches, with backpressure, Prometheus metrics and a drain on shutdown.

## Run

```sh
docker compose -f apps/11-clickhouse-sink/docker-compose.yml up -d --wait && go run ./apps/11-clickhouse-sink/cmd/clickhouse-sink
go test -race -tags integration ./apps/11-clickhouse-sink/...
```

## Where to start

1. `cmd/clickhouse-sink/main.go:run` — the wiring: env config, ClickHouse store, metrics registry, batcher, HTTP handler.
2. `cmd/clickhouse-sink/main.go:serve` — the lifecycle: two goroutines, and the order in which they are stopped.
3. `internal/server/server.go:handleIngest` — the API contract: decode, validate, enqueue, and the status code for each failure.
4. `internal/batcher/batcher.go:Enqueue` — backpressure: how a request is accepted whole or not at all without blocking.
5. `internal/batcher/batcher.go:Run` — the single consumer: size and interval flushes, retries, and the drain.

## Data flow

1. `POST /ingest` reaches `handleIngest`, which decodes the body into `[]event.Event` through a 4 MiB `MaxBytesReader`.
2. Every event is validated by `event.Event.Validate`; one bad event rejects the whole request with 422.
3. `Batcher.Enqueue` checks free capacity under a mutex and pushes all events into a buffered channel, or returns `ErrBufferFull` (429 + `Retry-After`).
4. The `Run` goroutine receives from the channel into a local slice and calls `flush` when the slice reaches `BATCH_SIZE` or the ticker fires.
5. `flush` calls `Store.InsertEvents` (`PrepareBatch` → `Append` per row → `Send`), retrying with exponential backoff up to `FLUSH_MAX_ATTEMPTS`.
6. On SIGTERM `serve` shuts the HTTP server down first, then cancels the batcher, which closes the channel, flushes what is left and returns.
7. `/metrics` renders the custom registry; `/health` pings ClickHouse with a 2 s timeout.

## Go specifics here

1. `internal/batcher/batcher.go:119` — capacity check before a send loop.
   <details><summary>Explanation</summary>

   `len(ch)` and `cap(ch)` on a buffered channel return the number of queued values and the capacity. On its own, "check then send" is racy, but here both the check and the sends run under `b.mu`, so no other producer can take free slots in between. The only other party is the `Run` goroutine, which only receives, so free space can grow but never shrink during the loop. That is why the sends at line 123 never block and why a request is either fully enqueued or not at all.
   </details>

2. `internal/batcher/batcher.go:132` — `context.WithoutCancel`.
   <details><summary>Explanation</summary>

   `WithoutCancel` returns a context that keeps the parent's values but is never cancelled when the parent is. `Run` watches `ctx` to know when to stop, but the inserts themselves use `flushCtx`, so an insert already in progress is not aborted halfway by shutdown. The drain then wraps it in its own `WithTimeout`, which bounds how long the final flush may take.
   </details>

3. `internal/batcher/batcher.go:171` — `range` over a channel.
   <details><summary>Explanation</summary>

   `for e := range ch` receives until the channel is closed and empty, then exits. The channel is closed at line 162 while holding the mutex and after setting `closed`, so `Enqueue` can no longer send (sending on a closed channel panics). The loop therefore reads exactly the values that were buffered at the moment of closing — no polling or `len` checks needed. Closing is the sender's job in Go; here the consumer does it, which is safe only because the mutex serialises it with every send.
   </details>

4. `internal/storage/storage.go:75` — named result and a deferred closure.
   <details><summary>Explanation</summary>

   The signature names its result `err`. A deferred function runs after the `return` statement has assigned the result but before the caller sees it, and because the closure refers to the named variable it can still change it. Here it joins a `batch.Close()` failure into whatever error the function was returning. The inner `if err := ...` statements declare new, shadowing variables; their values reach the named result only through the explicit `return`.
   </details>

5. `cmd/clickhouse-sink/main.go:136` — a buffered channel of size 1 for a goroutine result.
   <details><summary>Explanation</summary>

   A goroutine cannot return a value, so its result is sent on a channel. With an unbuffered channel the send blocks until someone receives; if `serve` returned early on another path, the goroutine would block forever and leak. A buffer of one lets the goroutine deposit its single result and exit whether or not anyone ever reads it. The same pattern is used for `serveErr` on line 139.
   </details>

## Questions

1. Why can `Enqueue` send many events into the channel while holding the mutex without ever blocking, even though `Run` is concurrently receiving?
   <details><summary>Answer</summary>

   Producers are serialised by `b.mu`, and the free-space check happens under the same lock as the sends. The single consumer only removes values, so between the check and the last send the free space can only grow. A blocking send is impossible, so holding the lock while sending cannot deadlock.
   </details>

2. With `BUFFER_SIZE=4` and `BATCH_SIZE=2`, how many accepted events can be held in memory at once, and why does `sink_buffer_length` not always show all of them?
   <details><summary>Answer</summary>

   Up to six: four in the channel and two in `Run`'s local `batch` slice. While a flush is retrying against an unavailable ClickHouse, `Run` does not receive, so the channel fills to capacity on top of the batch being flushed. The gauge reports `len(b.events)`, the channel only, so the in-flight batch is invisible to it.
   </details>

3. A client gets 202, then ClickHouse stays down longer than the retry budget. What happens to those events, and how would an operator notice?
   <details><summary>Answer</summary>

   After `FLUSH_MAX_ATTEMPTS` failed inserts the batch is dropped: `sink_flush_errors_total` grows per attempt, `sink_events_dropped_total` by the batch size, and a `batch dropped` error is logged. The handler's doc comment is explicit that 202 means "buffered", so delivery is at-most-once. Stronger guarantees would need a disk spool or acknowledging only after the insert.
   </details>

4. Why does `serve` call `srv.Shutdown` before `stopBatcher`, and why is `batcherCtx` derived from `context.WithoutCancel(ctx)` rather than from `ctx`?
   <details><summary>Answer</summary>

   `Shutdown` waits for in-flight handlers, which may still call `Enqueue`. If the batcher drained first, those requests would get `ErrClosed` (503) even though the process was still serving them. Deriving from `ctx` would cancel the batcher at the same moment as the signal, racing the drain against the HTTP shutdown; `WithoutCancel` plus an explicit `stopBatcher()` makes the order deterministic.
   </details>

5. A client times out after the server already returned 202 and retries the same request. What ends up in ClickHouse, and what in the schema could deal with it?
   <details><summary>Answer</summary>

   Both copies are stored: plain `MergeTree` never deduplicates. `event_id` is required precisely so duplicates are identifiable — queries can use `uniqExact(event_id)` (as the integration test does), or the table could use `ReplacingMergeTree` with `event_id` in the sorting key so background merges collapse repeats.
   </details>
