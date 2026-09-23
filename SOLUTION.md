# 012 · pipeline: dead-letter only the events ClickHouse rejects

5 defects, 2 decoys.

## 1. The wrong record is dead-lettered and the refused event is lost (high)

`apps/12-eventsink/internal/pipeline/pipeline.go:257`, in `flush`

What goes wrong: `for i := range rejected` yields positions in `rejected`, but `valid[i]` needs the
refused event's index into the batch, which is `rejected[i].index`. For a batch `b, a, c` in which
ClickHouse refuses `a`, `rejected` is `[{index: 1}]`, so the loop dead-letters `valid[0]` (`b`, already
stored in ClickHouse). `a` then goes nowhere: it is neither in ClickHouse nor in the dead-letter topic,
and the commit covers its offset, so it is lost. With k refusals, the first k valid records are
dead-lettered in their place. This is the delivery-semantics defect.

The tell: `valid[i]` next to `rejected[i].err` in the same call, although `rejection` has an `index`
field for exactly this and `insert` keeps it right with `right[i].index += mid`. The tests only refuse
the first event, or every event, which are the two cases where position and index coincide.

Fix:

```go
		for _, r := range rejected {
			dead = append(dead, p.deadLetter(valid[r.index], r.err))
		}
```

Rule: `for i := range s` yields positions in `s`; when elements carry an index into another slice, use
that index, and test with the interesting element somewhere other than first.

Found if: the verdict says the dead-letter loop picks records by position in `rejected` instead of by
`index`, so the wrong record goes to the DLQ and the refused one is committed without being stored.

## 2. Splitting runs the full retry policy on every half, and splits on outages too (high)

`apps/12-eventsink/internal/pipeline/pipeline.go:301`, in `insert` (the recursion is at 314 and 318)

What goes wrong: `insert` cannot tell "ClickHouse is down" from "this event is bad": any insert that
runs out of attempts is split, and every half goes through `p.retry` with `MAX_ATTEMPTS` attempts and
backoff again. During an outage every half fails as well, so a batch of 1000 is split down to single
events: 1999 retried inserts with about 3 s of backoff each, roughly 100 minutes, or about 10 hours if
every attempt runs into `ATTEMPT_TIMEOUT`, and some 10 000 error log lines.

Rebalances stay blocked the whole time. After 60 s the group drops the member, and the member that takes
over replays the batch into the same storm; at the end every event is dead-lettered anyway, as before
the change. Even a real poison event costs a full round of attempts at each of about 10 levels, so two in
one batch overrun the 60 s rebalance timeout. The README's "44 s" worst case no longer holds, and the
diff leaves it as is.

The tell: a retry loop with backoff inside a recursive split, and nothing that classifies the error
before splitting. The branch's own case "insert that hangs times out on every attempt and is
dead-lettered" shows an attempt timeout treated as a refusal.

Fix: split only on an error that says the data was refused, and try each half once. For example, the
warehouse wraps ClickHouse's data exceptions in a sentinel:

```go
	case !errors.Is(err, warehouse.ErrRefused):
		return nil, err // an outage: the whole batch goes to the DLQ, as before
	...
	left, err := p.insertOnce(ctx, events[:mid]) // p.attempt, no retry, no backoff
```

Rule: before splitting, dead-lettering or retrying on failure, tell transient errors from permanent
ones; retries are for the first, bisecting for the second.

Found if: the verdict says splitting also happens when ClickHouse is simply down (or that retries inside
the recursion multiply the time a batch holds rebalances).

## 3. The new ledger status violates the `batches` CHECK constraint (high)

`apps/12-eventsink/internal/ledger/ledger.go:32`, used in `flush` at `pipeline.go:252`

What goes wrong: migration `00001_create_batches.sql` defines
`CHECK (status IN ('inserted', 'dead_lettered'))`, and the PR adds no migration. The first batch with a
partial refusal makes `Ledger.Record` fail with SQLSTATE 23514 (checked against Postgres 16), which no
retry can fix. `flush` returns "record batch", the process exits 1, restarts, reads the same
uncommitted batch and fails again: a crash loop that stalls the partition on every member it moves to.
Until this is fixed it also hides defect 1, because no partial batch is ever committed.

The tell: a new value of an enum that is written to a database, and no migration in the diff.

Fix:

```sql
-- +goose Up
ALTER TABLE batches DROP CONSTRAINT batches_status_check;
ALTER TABLE batches ADD CONSTRAINT batches_status_check
    CHECK (status IN ('inserted', 'partially_dead_lettered', 'dead_lettered'));
```

Rule: a new value of a stored enum ships with its schema change in the same change.

Found if: the verdict names `partially_dead_lettered` against the table's CHECK constraint, or the
missing migration.

## 4. A gauge for a count of events (medium)

`apps/12-eventsink/internal/pipeline/pipeline.go:249`, in `flush` (declared at 127, in `New`)

What goes wrong: `eventsink_rejected_events` is a `Gauge`, and every flush calls
`Set(len(rejected))`. A flush that refuses 3 events is followed 2 s later by a clean one that sets 0, so
a 15 s scrape almost never sees the 3. An alert on `> 0` fires by luck, and `increase()` cannot count
refusals. It also duplicates `eventsink_dead_letters_total{reason="insert_failed"}`, which counts them
correctly.

The tell: `Set` with a per-flush count, and help text ("Events the Inserter refused") that describes a
total.

Fix:

```go
		rejected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "rejected_events_total", Help: "Events the Inserter refused even on their own.",
		}),
	...
	p.metrics.rejected.Add(float64(len(rejected)))
```

Rule: things that happen are counters, read with `rate` or `increase`; gauges hold current state.

Found if: the verdict says the rejected-events metric should be a counter, or that `Set` per flush loses
values between scrapes.

## 5. The "not split" test cannot see a split (medium)

`apps/12-eventsink/internal/pipeline/pipeline_test.go:255`, in `TestPipelineRun`

What goes wrong: the case "insert that recovers after max attempts is not split" makes the fake fail the
first 3 calls, and `MaxAttempts` is 3 (line 395). The whole batch therefore runs out of attempts, which
is exactly what triggers a split, and the batch is split into `a` and `b` (5 calls in all). The case
asserts only the inserted IDs and the commit, which are the same with or without a split. It is what
keeps defect 2 green.

The tell: `insertFailures` equal to `MaxAttempts`, and no assertion on the number of calls or the size
of the inserts.

Fix: assert the claim (a `wantCalls` field compared with `inserter.calls`), which with the current code
shows 5 calls. Then either stop splitting on this error (defect 2) or rename the case after what it
tests.

Rule: a test's name is a claim, and every claim needs an assertion that would fail without it.

Found if: the verdict says this case does split (3 failures against 3 attempts), or that nothing in it
checks splitting.

## Not defects

### Appending to a nil slice

`apps/12-eventsink/internal/pipeline/pipeline.go:325`: `left` is nil whenever the left half was stored
(`return nil, nil` at 299 and 306), and `append(left, right...)` appends to it. In Go a nil slice is a
valid empty slice: `append` allocates as needed and `len(nil)` is 0, so returning `nil, nil` for "no
refusals" is idiomatic.

### Updating elements through the index

`apps/12-eventsink/internal/pipeline/pipeline.go:322-323`: `for i := range right { right[i].index += mid }`
changes the elements in place. It looks like mutating a collection while iterating it, which is safe
here: the range only reads the length, and assigning through `right[i]` writes into the slice's array.
`for _, r := range right { r.index += mid }` would be the bug, because `r` is a copy.

## Also acceptable

- `eventsink_dead_letters_total{reason="insert_failed"}` used to count whole batches lost to an outage
  and now also counts single refused events, so the label no longer tells an outage from bad data.
- The README's rebalance paragraph still gives a 44 s worst case that the splitting no longer respects
  (the root of it is defect 2).
- Every stored half is its own `INSERT`, and so its own ClickHouse part: one refused event in a batch of
  1000 adds about ten parts, and many refusals push the table towards `TOO_MANY_PARTS`.
- The "dead-lettering rejected events" log line no longer carries an error, and `retry` logs every failed
  attempt of every half at ERROR without its size.
- `rejected_events` is set before the dead letters are produced and the batch recorded and committed, so
  it counts refusals of a flush that later fails (secondary to defect 4).
- The app's READING.md still describes the old flush, which dead-lettered the whole batch.
