# 12-eventsink

A Kafka-to-ClickHouse sink on franz-go that batches JSON events by size or timeout, records every batch in Postgres, dead-letters what it cannot store and commits offsets only after both writes, with `/health` and `/metrics` next to it.

## Run

```sh
docker compose -f apps/12-eventsink/docker-compose.yml -p gr-12 up -d --wait && go run ./apps/12-eventsink/cmd/eventsink
go test -race -tags integration ./apps/12-eventsink/...
```

## Where to start

1. `cmd/eventsink/main.go:run` — the wiring, the order of the `defer`s (which is the shutdown order), and the HTTP server living around `Pipeline.Run`.
2. `internal/pipeline/pipeline.go:Run` — the single-goroutine loop: poll, flush when full or late, allow rebalances between batches, drain on shutdown.
3. `internal/pipeline/pipeline.go:flush` — the four steps of a flush and the order that makes commit-after-write hold.
4. `internal/warehouse/warehouse.go:Open` — the ClickHouse table definition that the idempotency story rests on.
5. `internal/ledger/ledger.go:Record` and `internal/server/server.go:handleHealth` — the Postgres batch log and the per-dependency health check.

## Data flow

1. `run` builds the pgx pool (and applies the embedded goose migrations), the ClickHouse store, the kgo client and the Prometheus registry, then starts the HTTP server and calls `Pipeline.Run`.
2. `poll` asks kgo for at most the room left in the batch, with a deadline at the moment the pending batch times out; every record is decoded by `event.Decode` and kept together with its decode error.
3. When the batch holds `BATCH_SIZE` records or its deadline has passed, `flush` splits it into valid events and dead letters.
4. The valid events go to `Store.InsertEvents` through `retry`; after the last failed attempt they join the dead letters.
5. Dead letters are produced to the DLQ topic with headers pointing at the original topic, partition and offset.
6. `Ledger.Record` inserts one `batches` row with the status, counts, insert duration and per-partition offset spans (retried like the insert).
7. Only then does `CommitRecords` commit the batch, and `Run` calls `AllowRebalance`.
8. On SIGTERM the loop exits, the pending batch is flushed under a context that outlives the signal by `SHUTDOWN_TIMEOUT`, the HTTP server shuts down, and the deferred closes leave the group and close ClickHouse and Postgres.

## Go specifics here

1. `cmd/eventsink/main.go:84` — a slice of options spread into a variadic call

<details><summary>Explanation</summary>

`kgo.NewClient(opts ...kgo.Opt)` takes a variadic list of functional options: each `kgo.Opt` is a value that knows how to modify the client configuration, the Go replacement for a long list of keyword arguments with defaults. `pipeline.ClientOptions` returns the options the pipeline depends on as a `[]kgo.Opt`, `append` adds the seed brokers, and the trailing `...` spreads the slice into the variadic parameter, like `*args` in a Python call. Keeping the group options next to the code that relies on them means `main` cannot build a client that silently auto-commits.

</details>

2. `internal/pipeline/pipeline.go:141` — a callback that schedules a callback

<details><summary>Explanation</summary>

`flushCtx` is derived from `context.WithoutCancel(ctx)`, so it keeps the values of `ctx` but not its cancellation. `context.AfterFunc(ctx, f)` runs `f` in its own goroutine once `ctx` is done (here: SIGTERM), and `f` uses `time.AfterFunc` to call `cancel` after the grace period. A flush that is running when the signal arrives therefore gets `SHUTDOWN_TIMEOUT` to finish instead of being cut off mid-insert. `stop` unregisters the first callback when `Run` returns early; if the timer was already armed, calling `cancel` a second time later is a no-op.

</details>

3. `internal/pipeline/pipeline.go:276` — two `%w` verbs

<details><summary>Explanation</summary>

`fmt.Errorf` may wrap several errors. The result matches both `errors.Is(err, errExhausted)` and `errors.Is(err, <last insert error>)`. `flush` branches on the sentinel at line 229 to decide on dead-lettering, while the logs and the `dlq-error` header still carry the ClickHouse error text. It behaves more like matching inside an `ExceptionGroup` than like following a single `__cause__` chain.

</details>

4. `internal/server/server.go:51` — a goroutine per loop iteration

<details><summary>Explanation</summary>

Since Go 1.22 every iteration of a `for ... range` loop has its own `name` and `dep`, so each goroutine's closure sees its own dependency; there is no late-binding trap like a Python `lambda` created in a loop. The channel is buffered with one slot per goroutine, so every send completes immediately even if nobody is receiving yet, and the handler collects exactly `len(deps)` results with `for range len(deps)`. All pings share one two-second deadline and run concurrently, so one hanging dependency cannot make the others look slow.

</details>

5. `internal/warehouse/warehouse.go:54` — a named result assigned inside `defer`

<details><summary>Explanation</summary>

The result is declared as `(err error)`, so it is a variable that exists for the whole function. The deferred closure runs after the `return` statement has set it, and can still change it: line 64 joins a failure of `batch.Close` into whatever error the function was about to return. Without the named result, the error from closing would be lost. Python has no direct equivalent: a `finally` block can replace the return value with its own `return`, but it cannot see the value that was about to be returned.

</details>

## Questions

1. The process is killed after `InsertEvents` succeeded and before `CommitRecords` ran. What happens after the restart in ClickHouse, in the `batches` table and in the DLQ topic?

<details><summary>Answer</summary>

The group resumes from the last committed offset, so the whole batch is read again, possibly split into different batches. ClickHouse receives the same rows a second time; because the table is a `ReplacingMergeTree` whose sorting key identifies the event, the duplicates collapse (at once if they land in the same block, otherwise during a merge) and `SELECT ... FINAL` already returns one row per event. The `batches` table gets new rows covering the same offsets, because it is a log of flushes. If the batch contained invalid records, they are produced to the DLQ again. Nothing is lost; the duplicates exist only where no deduplication was designed in.

</details>

2. `retry` returned an error wrapping `errExhausted`, yet `flush` returns it instead of dead-lettering the batch when `ctx.Err() != nil`. Why?

<details><summary>Answer</summary>

`ctx` here is the flush context, which is canceled only when the shutdown grace period ends. At that point the last attempt most likely failed because the context was canceled, not because ClickHouse rejected the batch. Dead-lettering would move healthy events into the DLQ and commit them just because the service was stopping. Returning an error leaves the batch uncommitted, `run` exits with status 1, and the next group member inserts it normally.

</details>

3. Why does `Run` call `AllowRebalance` only when no batch is pending, and what does this imply for `BATCH_TIMEOUT`, `MAX_ATTEMPTS` and `RETRY_BACKOFF`?

<details><summary>Answer</summary>

With `BlockRebalanceOnPoll`, a rebalance waits until `AllowRebalance` is called. If partitions were revoked while polled records were still pending, the later `CommitRecords` could commit offsets for partitions another member now owns, and that member would process the same records concurrently. Allowing rebalances only between batches keeps every commit on owned partitions. The price is that a rebalance can wait for a full batch timeout plus every retry and backoff of the insert and the ledger write; that sum must stay below the group's rebalance timeout, or the member is kicked out of the group.

</details>

4. In the integration test's replay subtest, the first ClickHouse check uses `count() ... FINAL`, and a plain `count()` is only checked after `OPTIMIZE TABLE events FINAL`. Why not check `count()` right away?

<details><summary>Answer</summary>

After the replay, each event exists in two data parts: the one from the first run and the one from the replay. `ReplacingMergeTree` removes the older row only when those parts are merged, which ClickHouse does in the background at a time of its choosing, so a plain `count()` may still see both rows. `FINAL` deduplicates at query time and is correct immediately; `OPTIMIZE ... FINAL` forces the merge, after which the rows are physically gone and a plain `count()` agrees.

</details>

5. `/health` receives `*pgxpool.Pool` and `*kgo.Client` directly, even though neither library has heard of `server.Pinger`. Why does this compile, and why is `Pinger` declared in the `server` package rather than next to the ClickHouse store?

<details><summary>Answer</summary>

Go interfaces are satisfied implicitly: any type with a `Ping(context.Context) error` method is a `Pinger`, much like a `typing.Protocol`. Both library types happen to have that method, and so does `warehouse.Store`. Declaring the interface where it is consumed keeps it as small as the handler needs, lets the tests pass trivial fakes (including one that blocks until the deadline), and keeps `warehouse`, `pgxpool` and `kgo` free of any dependency on the HTTP layer.

</details>
