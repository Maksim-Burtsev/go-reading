# 07 · kafka-consumer

A Kafka consumer-group member built on franz-go (`kgo`). It reads JSON events from the `events`
topic and writes them to stdout as JSON lines in batches of up to 100 events or one second,
committing offsets only after a batch has been written. Records that are not valid events, and
batches the sink still rejects after three attempts, go to the `events.dlq` topic instead, and on
SIGTERM the pending batch is flushed and committed before the member leaves the group.

## Run it

```sh
docker compose -f apps/07-kafka-consumer/docker-compose.yml up -d --wait
go run ./apps/07-kafka-consumer/cmd/kafka-consumer
```

In a second terminal:

```sh
echo '{"id":"e-1","type":"order.created","occurred_at":"2026-09-21T10:00:00Z"}' | docker compose -f apps/07-kafka-consumer/docker-compose.yml exec -T redpanda rpk topic produce events
echo 'not json' | docker compose -f apps/07-kafka-consumer/docker-compose.yml exec -T redpanda rpk topic produce events
docker compose -f apps/07-kafka-consumer/docker-compose.yml exec redpanda rpk topic consume events.dlq --num 1
```

Events go to stdout and logs to stderr. The valid event is written about a second after it arrives
(`BATCH_TIMEOUT`); the invalid one is logged and lands in `events.dlq` with `dlq-*` headers.

## Questions

Answer from the code first, then open the answer.

1. `run` passes a `*kgo.Client` to `consumer.New`, whose parameter is a `consumer.Client`, and
   nothing in kgo mentions that interface. Why does this compile, and what does declaring the
   interface in package `consumer` buy?

   <details><summary>Answer</summary>

   Interfaces are satisfied implicitly: any type with the listed methods is a `consumer.Client`, and
   `*kgo.Client` has `PollRecords`, `ProduceSync`, `CommitRecords` and `AllowRebalance` with
   matching signatures; it works like a `typing.Protocol` that the compiler checks. The check
   happens where the value is converted to the interface, so if kgo changed one of those signatures
   the error would appear at the `consumer.New` call in `run`. Declared next to its only user, the
   interface lists just the four methods the consumer calls, which is what lets the unit tests pass
   a small in-memory fake instead of a broker. Rule: declare interfaces on the consuming side, only
   as large as the consumer needs.

   </details>

2. `ClientOptions` turns auto-commit off. What would go wrong if this consumer kept kgo's default
   auto-commit?

   <details><summary>Answer</summary>

   With auto-commit on, kgo commits in the background every five seconds, and each `PollRecords`
   call marks the records returned by the previous poll as ready to commit, assuming a poll's
   records are processed before the next poll. This consumer polls many times while one batch fills,
   so records from earlier polls could be committed while they still sit unwritten in the batch; a
   crash at that moment loses them, because the group resumes after the committed offset. With
   auto-commit off, the only commit is the `CommitRecords` call at the end of `flush`, after the
   sink write or the dead-letter produce. Rule: commit an offset after the side effect it stands
   for, from the code that performed it, never on a timer.

   </details>

3. There is no ticker goroutine. How does a partial batch get flushed one second after its first
   record arrived?

   <details><summary>Answer</summary>

   `pollContext` gives each poll a context whose deadline is the batch deadline: the first record's
   arrival time plus `BATCH_TIMEOUT`. `PollRecords` blocks until records arrive or that context
   ends; at the deadline it returns a fetch carrying `context.DeadlineExceeded`, which `poll` skips,
   then `Batcher.Due` reports true and `Run` flushes. With an empty batch the poll has no deadline
   and blocks until records arrive or the service stops. Rule: when a loop already blocks on a
   context-aware call, a deadline on that call's context replaces a timer goroutine and the locking
   it would need.

   </details>

4. Why does `Run` call `AllowRebalance` only when the batch is empty, rather than after every poll?

   <details><summary>Answer</summary>

   With `BlockRebalanceOnPoll`, a poll that returns records holds rebalances back until
   `AllowRebalance` is called. Released while polled records still sit in the batch, the gate would
   let the group move their partition to another member, which starts from the last committed offset
   and processes them again, while this member's later `CommitRecords` commits a partition it no
   longer owns and can move the new owner's offset backwards. After a flush, an empty batch means
   everything polled has been handled and committed. The cost is that a rebalance waits up to one
   batch timeout plus a flush, which must stay well under the group's rebalance timeout. Rule:
   partition ownership must not change between reading records and committing them.

   </details>

5. An invalid record at offset 10 is decoded while offsets 5 to 9 of the same partition are still
   waiting in the batch. Why is it not dead-lettered and committed right away?

   <details><summary>Answer</summary>

   A Kafka commit is a position, not a per-record acknowledgement: `CommitRecords` for offset 10
   commits 11, which tells the group that everything before 11 in that partition is done. Doing that
   now would mark 5 to 9 as done before they reach the sink, and a crash would lose them. So the
   invalid record stays in the batch with its decode error, goes to the dead-letter topic during
   `flush`, and is committed together with the rest. Rule: with offset commits, a record can be
   committed only when every earlier record of its partition is finished.

   </details>

6. SIGTERM arrives while the sink is failing and `retry.Policy.Do` is waiting before its second
   attempt. What happens next?

   <details><summary>Answer</summary>

   `Run` flushes with `flushCtx`, not `ctx`. `withGrace` builds it on `context.WithoutCancel(ctx)`,
   so the signal does not cancel it, and registers `context.AfterFunc(ctx, ...)`, which cancels it
   `SHUTDOWN_TIMEOUT` after the signal. If the attempts run out inside that window, the batch is
   dead-lettered and committed, and the loop exits into an empty final flush. If the window closes
   first, the backoff sleep returns `context.Canceled`, `flush` returns an error without producing
   or committing, and the process exits with status 1; the uncommitted records are delivered again
   to whichever member gets the partition next. Rule: shutdown work needs a context detached from
   the shutdown signal and bounded by its own deadline.

   </details>

7. `flush` dead-letters a failed batch only when `errors.Is(err, retry.ErrExhausted)` holds and
   `ctx.Err()` is nil. What does each half of that condition protect against?

   <details><summary>Answer</summary>

   `Do` joins two errors with two `%w` verbs: `ErrExhausted` and the last sink error when every
   attempt failed, or the context error and the last sink error when a backoff wait was cut short.
   `errors.Is` searches both, so the first check separates "the sink kept rejecting this batch",
   worth dead-lettering, from "we were interrupted", which must leave the batch uncommitted. The
   `ctx.Err()` check covers the grace period ending during the last attempt: the sink returns
   `context.Canceled`, `Do` has no attempts left and reports `ErrExhausted`, and healthy events
   would be dead-lettered only because time ran out. Rule: dead-letter only failures caused by the
   data or the destination, never your own cancellation.

   </details>

8. `Batcher.Drain` returns its slice and starts a new one with `make`. What would change if it
   reset with `b.items = b.items[:0]`, and why does `deadLetter` clone the original headers before
   appending to them?

   <details><summary>Answer</summary>

   A slice is a pointer, a length and a capacity over a shared backing array. After
   `b.items = b.items[:0]`, the next `Add` would write into the array the caller of `Drain` still
   holds and overwrite its first element. `flush` is done with a batch before `poll` adds again, so
   nothing breaks today, but any caller that keeps a batch, such as a background flusher, would see
   it change underneath; `make` gives the caller sole ownership. `append` writes in place when
   capacity allows, so appending to `r.Headers` could write into memory another slice shares, and
   `slices.Clone` rules that out. Rule: know who owns a slice's backing array before you append to
   it or reuse it; when in doubt, copy.

   </details>
