# 07-kafka-consumer

A Kafka consumer-group service on franz-go that batches JSON events into a stdout sink, commits offsets only after a batch is written or dead-lettered, and flushes the pending batch on shutdown.

## Run

```sh
docker compose -f apps/07-kafka-consumer/docker-compose.yml up -d --wait && go run ./apps/07-kafka-consumer/cmd/kafka-consumer
echo '{"id":"e-1","type":"order.created","occurred_at":"2026-09-21T10:00:00Z"}' | docker compose -f apps/07-kafka-consumer/docker-compose.yml exec -T redpanda rpk topic produce events
go test -race -tags integration ./apps/07-kafka-consumer/...
```

## Where to start

1. `cmd/kafka-consumer/main.go:run` — wiring: env config, the kgo client, topic creation, and how the consumer is assembled and closed.
2. `internal/consumer/consumer.go:ClientOptions` — the group options (manual commits, blocked rebalances) that the rest of the package depends on.
3. `internal/consumer/consumer.go:Run` — the single-goroutine loop: poll, flush when due, allow rebalances, drain on shutdown.
4. `internal/consumer/consumer.go:flush` — sink write with retries, the dead-letter decision, and the commit.
5. `internal/batch/batch.go:Batcher` and `internal/retry/retry.go:Do` — the two pure building blocks the loop is made of.

## Data flow

1. `Run` calls `poll`, which asks kgo for at most `Room()` records, with a deadline at the moment the pending batch times out.
2. Each record is decoded by `event.Decode`; the record, the event and any decode error go into the batch together.
3. When the batch is full or its deadline has passed, `flush` drains it and splits valid events from invalid ones.
4. Valid events go to `Sink.Write` through `retry.Policy.Do`; after the last failed attempt they join the invalid records.
5. Every dead record is produced to the DLQ topic with headers pointing at its original topic, partition and offset.
6. Only then are all records of the batch committed with `CommitRecords`, and `Run` calls `AllowRebalance`.
7. On SIGTERM the poll context is canceled, the loop exits, and the pending batch is flushed under a context that lives for `SHUTDOWN_TIMEOUT` longer; `main` then closes the client, which leaves the group.

## Go specifics here

1. `internal/consumer/consumer.go:36` — an interface kgo never mentions

<details><summary>Explanation</summary>

Go interfaces are satisfied implicitly: `*kgo.Client` has these four methods, so it is a `consumer.Client` without declaring anything, much like a `typing.Protocol`. The interface lives next to its user and lists only what the consumer calls, which is why the unit tests can hand `Run` a small fake instead of a broker.

</details>

2. `internal/consumer/consumer.go:234` — `WithoutCancel` plus `AfterFunc`

<details><summary>Explanation</summary>

Cancellation in Go flows down a tree of contexts: canceling a parent cancels every child. `context.WithoutCancel` detaches from that, keeping values but not cancellation, and `context.AfterFunc` runs a callback in its own goroutine once the parent is done. Together they build a context that survives SIGTERM for exactly the grace period, so a flush that is already running is not cut off the moment the signal arrives. The returned cancel func unregisters the callback, or ends it early if it is already waiting.

</details>

3. `internal/retry/retry.go:32` — a generic function stored in a field

<details><summary>Explanation</summary>

`rand.N` is generic (`func N[Int intType](n Int) Int`). Writing `rand.N[time.Duration]` instantiates it without calling it, producing an ordinary `func(time.Duration) time.Duration` value. Tests in the same package overwrite the unexported `jitter` and `sleep` fields to make backoff deterministic; there is no monkeypatching of module globals.

</details>

4. `internal/retry/retry.go:52` — two `%w` verbs

<details><summary>Explanation</summary>

Since Go 1.20 `fmt.Errorf` may wrap several errors. The result matches both `errors.Is(err, retry.ErrExhausted)` and `errors.Is(err, <last sink error>)`. The consumer branches on the sentinel to decide on dead-lettering, while the logged message still carries the sink's own error text. It behaves more like matching inside an `ExceptionGroup` than like a single `__cause__` chain.

</details>

5. `internal/batch/batch.go:62` — a fresh slice on every drain

<details><summary>Explanation</summary>

A slice is a view over a backing array. `Drain` hands the current slice to the caller; if it then reused the same array with `b.items = b.items[:0]`, the next `Add` would overwrite elements the caller is still iterating over. Allocating a new slice keeps the returned batch stable. The same aliasing concern is why `deadLetter` clones the original headers before appending to them.

</details>

## Questions

1. Why does `Run` call `AllowRebalance` only when the batch is empty, rather than after every poll?

<details><summary>Answer</summary>

With `BlockRebalanceOnPoll`, a rebalance waits until `AllowRebalance` is called. If it were allowed while polled records were still uncommitted, partitions could be revoked and handed to another member, and the later `CommitRecords` would commit offsets for partitions this member no longer owns, possibly rewinding the new owner's progress, while the new owner processes the same records again. Allowing it only between batches means every commit covers partitions the member still holds. The cost is that a rebalance can wait up to one batch timeout plus the sink retries, which must stay below the group's rebalance timeout.

</details>

2. There is no ticker goroutine. How does a partial batch get flushed after one second?

<details><summary>Answer</summary>

`pollContext` derives the poll context from the batch deadline, which is the first item's arrival time plus `BATCH_TIMEOUT`. `PollRecords` returns with `context.DeadlineExceeded` when that deadline passes (the error is skipped in `poll`), and `Batcher.Due` then reports true, so `Run` flushes. With an empty batch the poll has no deadline and blocks until records arrive or the service is stopped.

</details>

3. An invalid record at offset 10 is detected while offsets 5–9 of the same partition are waiting in the batch. Why is it not dead-lettered and committed right away?

<details><summary>Answer</summary>

`CommitRecords` commits the highest offset per partition. Committing offset 10 would also mark 5–9 as done even though they have not reached the sink yet; a crash at that point would lose them. The invalid record therefore stays in the batch, is produced to the DLQ during `flush`, and is committed together with the rest of the batch.

</details>

4. SIGTERM arrives while the sink is failing and `retry.Policy.Do` is waiting before the second attempt. What happens next?

<details><summary>Answer</summary>

Nothing is interrupted immediately: `flush` runs under `flushCtx`, which is only canceled `SHUTDOWN_TIMEOUT` after the signal. If the attempts are exhausted within that window, the batch goes to the DLQ and is committed; the loop then sees the canceled context and does a final, usually empty, flush. If the window closes first, the backoff sleep returns `context.Canceled`, `flush` returns an error without producing or committing (the `ctx.Err() != nil` guard also stops a last-attempt failure from being dead-lettered), `run` exits with status 1, and the records are delivered again to the next group member.

</details>

5. Why does `main` defer `client.CloseAllowingRebalance()` instead of `client.Close()`?

<details><summary>Answer</summary>

`Close` leaves the group, and leaving waits for the rebalance gate. If `Run` returns an error in the middle of a batch (for example, the DLQ produce failed), the last poll is still holding rebalances back and plain `Close` would wait forever. `CloseAllowingRebalance` releases the gate first. Nothing is lost by doing so, because that batch was never committed.

</details>
