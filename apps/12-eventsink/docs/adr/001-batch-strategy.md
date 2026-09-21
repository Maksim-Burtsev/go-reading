# ADR-001: Batch strategy

## Status

Accepted, 2026-09-21.

## Context

eventsink moves JSON events from a Kafka topic into a ClickHouse table. Several constraints shape how
records are grouped and when their offsets are committed.

- Every ClickHouse `INSERT` creates at least one data part, and background merges have to keep up
  with the part count. ClickHouse recommends inserting at least a thousand rows at a time and about
  one insert per second per table; thousands of single-row inserts per second end in
  `TOO_MANY_PARTS`.
- A Kafka consumer group commits one offset per partition, meaning "everything before this is done".
  A consumer can only commit a prefix of what it has read.
- There is no transaction that spans Kafka, ClickHouse and Postgres. A process can die between any
  two of its writes.
- Events carry an `id` assigned by the producer. Producers retry, so the topic itself may already
  contain the same event twice.
- Operators want to answer "what happened to offset N of partition P" and "how long do inserts take"
  with plain SQL, including for batches that never reached ClickHouse.
- A malformed record, or a ClickHouse outage, must not stop the partition forever.

## Decision

**Size-plus-timeout batching.** A batch is flushed when it holds `BATCH_SIZE` records or when its
first record has waited `BATCH_TIMEOUT`, whichever comes first. One goroutine polls, batches and
flushes; the poll call is bounded by the batch deadline, so no timer goroutine is needed. At most
one batch is in flight.

**Commit after write.** A flush runs four steps in a fixed order: insert into ClickHouse, produce
dead letters, record the batch in Postgres, commit offsets. Offsets are committed only when the
first three steps succeeded. The consumer is created with `BlockRebalanceOnPoll` and allows a
rebalance only when no batch is pending, so a commit always covers partitions the member still owns.
A failed commit is logged and not retried: the next successful commit covers the same offsets.

**At-least-once delivery with an idempotent table.** A crash after the insert and before the commit
replays the batch, possibly with different batch boundaries. The `events` table is a
`ReplacingMergeTree(inserted_at)` whose sorting key `(event_type, occurred_at, event_id)` identifies
an event, so a replayed row and its original collapse into one: at insert time when they land in
the same block, otherwise during background merges. Queries that need exact results read with
`FINAL`, or aggregate with `uniqExact(event_id)` or `argMax(..., inserted_at)`. The same mechanism
removes duplicates that producers wrote to the topic.

**Dead-letter after N attempts.** The insert is retried up to `MAX_ATTEMPTS` times with exponential
backoff that starts at `RETRY_BACKOFF` and is capped at 10 s. When the attempts run out, every record
of the batch is produced to the DLQ topic with headers naming the original topic, partition, offset
and the last error, and the batch is committed. Records that do not decode as events never reach
ClickHouse: they go to the DLQ with the batch they arrived in, and are committed with it. If the
shutdown grace period ends while the insert is still failing, nothing is dead-lettered and nothing
is committed; the batch is replayed by the next group member.

**Batch metadata in Postgres.** Each flushed batch becomes one row in `batches`: status (`inserted`
or `dead_lettered`), record count, dead-letter count, insert duration, creation time, and a JSON
array with the topic, partition, first offset, last offset and record count of every partition the
batch touched. The ledger write is retried like the insert; if it still fails, the pipeline stops
without committing.

## Consequences

- ClickHouse receives few, large inserts. End-to-end latency is bounded by `BATCH_TIMEOUT` plus the
  flush time; memory is bounded by `BATCH_SIZE` records.
- No acknowledged event is lost: every failure before the commit leads to a replay, and the replay
  converges to one row per event in ClickHouse.
- Until merges run, `SELECT count() FROM events` can over-count. Readers have to know about `FINAL`
  or the deduplicating aggregates; this is the price of not coordinating writes across systems.
- An event is identified by its sorting key. A producer that reuses an `id` with a different
  `occurred_at` or `type` creates a second row. Changing the sorting key later means rebuilding the
  table.
- Replays leave duplicates where deduplication does not exist: the DLQ topic can hold the same
  record twice, and the ledger can hold two rows covering the same offsets. The ledger is a log of
  flushes, not a unique index of offsets.
- Rebalances wait for the batch in flight. `BATCH_TIMEOUT` plus the worst-case retry time must stay
  below the group's rebalance timeout.
- A ClickHouse outage longer than the retry budget moves traffic to the DLQ instead of stalling the
  partitions. Replaying the DLQ back into the input topic is an operational task outside this
  service.
- A Postgres outage stops the pipeline. Batches are replayed after a restart, and ClickHouse absorbs
  the duplicates.
- Throughput is bounded by one flush at a time per process. It scales with partitions and group
  members, not with goroutines inside one member.

## Alternatives considered

**Per-message insert and commit.** Simplest code and the lowest latency, but one ClickHouse part and
one offset commit per event. It collapses at a few hundred events per second. Rejected.

**Time-only batching.** Flushing on a fixed interval gives predictable latency, but under load the
batch size, the insert size and the memory held by the service are unbounded. Rejected.

**Size-only batching.** Optimal batches under steady load, but under light traffic a partial batch
waits indefinitely: events do not show up in ClickHouse, offsets are not committed, and rebalances
stay blocked. Rejected.

**ClickHouse asynchronous inserts.** `async_insert=1` moves batching into the server. With
`wait_for_async_insert=1` every insert still waits for the server-side flush, so commit-after-write
needs the same client-side grouping; without it the insert is acknowledged before it is durable and
the offsets could be committed for data that is later lost. It can be enabled on top of client
batching without changing this decision.

**Insert deduplication tokens.** ClickHouse can drop a repeated insert whose
`insert_deduplication_token` it has already seen. That only works if a replay reproduces the exact
same batch, and batch boundaries here depend on poll timing and on which member owns which
partitions after a rebalance. Rejected in favour of row-level deduplication, which does not care how
rows were grouped.

**Exactly-once by storing offsets with the data.** Writing offsets in the same transaction as the
data and seeking to them on partition assignment gives exactly-once for a transactional sink.
ClickHouse inserts are not transactional with anything else, so the scheme would cover the ledger
but not the events. Rejected.

**Blocking instead of dead-lettering.** Retrying forever keeps order and never needs a DLQ, but one
poison record or a long outage stops the partition and blocks rebalances. Rejected; the DLQ keeps
the pipeline moving and the ledger shows which batches went there.

**Batch metadata in ClickHouse.** One small insert per batch works against ClickHouse's part model,
and the ledger would be unavailable exactly when the warehouse it audits is down. Postgres takes
single-row writes cheaply, enforces the status and count constraints, and is where operational state
already lives. Rejected.

**One Postgres row per batch partition.** A separate `batch_partitions` table would make "which
batch covered offset N" an indexed lookup, at the cost of a transaction per flush. A batch is written
once and never updated, so a single row with a JSON array is enough for now; the side table can be
added when offset lookups become frequent.
