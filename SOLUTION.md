# 007 · consumer: poll the next batch while the previous one is written

4 defects, 2 decoys.

## 1. The hand-off drains the batch even when it is not sent (high)

`apps/07-kafka-consumer/internal/consumer/consumer.go:124`, in `Run`

What goes wrong: Go evaluates the channel and the value of every send case when it enters a
`select`, before it picks a case. `c.batch.Drain()` therefore runs and empties the batcher even
when the channel is full and `default` is taken, and the drained slice is discarded. The warning
says the batch is kept, but it is gone. With a slow sink (retries, a slow store), two batches wait
in the channel and the next full batch takes `default`. Its records are dropped, `Len()` is 0 so the
rebalance gate opens, and the next batch that fits is written and committed with higher offsets on
the same partitions. The dropped records are committed without ever being written or dead-lettered,
and `Run` returns nil. Measured with 30 records, batch size 1 and a 50 ms sink: 9 events written,
commits advanced to offset 28, 21 events silently lost.

The tell: a call with a side effect inside a send case, and a log line that reads `c.batch.Len()`
right after it, a value that is always 0 there.

Fix:

```go
		if c.batch.Due(time.Now()) {
			batches <- c.batch.Drain()
		}
```

A full channel should block the loop: that is the backpressure. To avoid blocking, check
`len(batches) < cap(batches)` before draining; the loop is the only sender, so the check cannot go
stale.

Rule: every channel operand and sent value of a `select` is evaluated before a case is chosen, so
never put a call with side effects in a send case that might not be taken.

Found if: names the `select` that sends `c.batch.Drain()` with a `default` and says the batch is
lost (drained even when it is not sent).

## 2. Rebalances are allowed while batches are still being written (medium)

`apps/07-kafka-consumer/internal/consumer/consumer.go:129`, in `Run`

What goes wrong: with `BlockRebalanceOnPoll`, calling `AllowRebalance` says that every polled
record has been committed. Before the change an empty batch meant exactly that. Now it only means
the records were handed to the flusher, and up to `PendingBatches` batches plus the one being
written are still uncommitted. When a rebalance starts in that window (a deploy, a scale-up), the
gate opens and a partition moves to another member, which resumes from the last committed offset
and processes those records again. Then this member's flusher commits them for a partition it no
longer owns, which can move the new owner's committed offset backwards and cause another round of
replays. Measured with a fake that counts polled-but-uncommitted records at every `AllowRebalance`:
2, 4, 4, 0, where master always has 0.

The tell: the gate condition is unchanged although its meaning changed, and `PR.md` asserts it is
fine ("allows rebalances only while its batch is empty, as before"). `fakeClient.AllowRebalance` is a
no-op, so no test can notice.

Fix: open the gate only when nothing polled is uncommitted, which now includes the flusher's work:

```go
		if c.batch.Len() == 0 && inFlight.Load() == 0 {
			c.client.AllowRebalance()
		}
```

Here `inFlight` is incremented on every hand-off and decremented by `flushLoop` after each `flush`.
`pollContext` must also give the poll a deadline while `inFlight > 0`; otherwise the loop can block
in `PollRecords` with the gate held.

Rule: a gate must be released only when the invariant it protects holds; when work moves to another
goroutine, the release condition has to follow it.

Found if: says that `AllowRebalance` now runs while handed-off batches are not yet committed, so a
rebalance can hand their partitions to another member (duplicates, commits to partitions no longer
owned).

## 3. A failed batch is skipped and later commits cover it (high)

`apps/07-kafka-consumer/internal/consumer/consumer.go:152`, in `flushLoop`

What goes wrong: before the change a failed flush ended `Run`, the process exited, and the
uncommitted batch was delivered again. Now `flushLoop` logs the error and moves on. A Kafka commit is
a position per partition, so the next successful batch on the same partitions commits past the
failed one. Suppose the sink exhausts its attempts on [a, b] and the dead-letter produce fails once
(a broker blip, an ACL change). Then [c, d] is written and committed at offset 3, and a and b are in
neither the sink nor the DLQ. The error surfaces only when the consumer stops, possibly hours later.
This contradicts the doc on `Run`, which the change keeps: "An error means some records were left
uncommitted and will be delivered again." The new case "a failed flush does not stop the consumer"
(`consumer_test.go:185`) asserts exactly this loss: commits `[[2 3]]` after the batch `[0 1]`
failed.

The tell: an error collected inside a long-running loop instead of returned, and a test whose
expected commits skip offsets that were never written.

Fix:

```go
func (c *Consumer) flushLoop(ctx context.Context, batches <-chan []pending) error {
	for items := range batches {
		if err := c.flush(ctx, items); err != nil {
			return err // and Run stops polling: select on flushed next to the hand-off
		}
	}
	return nil
}
```

Rule: a consumer that commits positions must never commit past a record it failed to handle; stop,
or retry that batch, but do not skip it.

Found if: says that the flusher carries on after a failed batch so a later commit covers it (loss),
or that the error only surfaces at shutdown, contradicting the doc on `Run`.

## 4. The "polled while written" test passes without the change (low)

`apps/07-kafka-consumer/internal/consumer/consumer_test.go:142`, in `TestConsumerRun`

What goes wrong: the case asserts the same sink output and commits as "full batches are flushed
before shutdown", only with a slower sink. Nothing in it observes a poll happening while a batch is
written, and it passes unchanged against master's sequential `Run`. It also produces at most two
full batches, which always fit in the channel, so it can never reach the `default` branch of
defect 1.

The tell: a copy of an existing case plus `sinkDelay`, with no assertion about overlap.

Fix: make the first write block until the fake client serves another poll, so the old sequential
loop fails the test, and add a case with more full batches than `PendingBatches`:

```go
func (s *blockingSink) Write(ctx context.Context, _ []event.Event) error {
	select {
	case <-s.release: // closed by the fake client when it serves the next poll
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Rule: a test for concurrent behavior has to fail on the sequential version; asserting only the
final state proves nothing about overlap.

Found if: says the case would also pass on the old sequential `Run`, or that it never observes
polling during a write.

## Not defects

### Handing the drained slice to another goroutine

`apps/07-kafka-consumer/internal/consumer/consumer.go:135`: the loop sends the slice returned by
`Drain` to the flusher (the same happens at line 124) and keeps calling `Add` on the same batcher.
That would be a data race if the two shared a backing array. They do not: `Drain` gives the batcher
a fresh array with `make` (`internal/batch/batch.go:61`), so the flusher owns the old one alone.

### Assigning to a field of the `cfg` parameter in `New`

`apps/07-kafka-consumer/internal/consumer/consumer.go:92`: `New` writes `cfg.PendingBatches`. In
Python that would change the caller's object; in Go a struct parameter is passed by value, so the
default lands in the consumer's own copy and the caller's `Config` is untouched.

## Also acceptable

- Up to `PendingBatches` + 2 batches are now uncommitted at once, so a crash replays more records,
  and the shutdown grace period has to cover several flushes instead of one.
- A negative `PendingBatches` passed to `New` makes `make` panic in `Run`; only `main` validates
  it, as it does for `BatchSize`.
- The behavior the warning describes would be broken too: with the batch kept, the next poll would
  call `PollRecords(ctx, c.batch.Room())` with `Room() == 0`, which kgo treats as "no limit", and a
  kept batch past its deadline gets an already expired poll context, so the loop spins.
- `flushLoop` keeps every flush error for the life of the process.
