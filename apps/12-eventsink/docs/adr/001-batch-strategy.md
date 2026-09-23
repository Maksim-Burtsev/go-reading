# ADR-001: Batch strategy

Accepted, 2026-09-21.

## Context

eventsink moves JSON events from a Kafka topic into a ClickHouse table. ClickHouse writes every
`INSERT` as a new data part and merges parts in the background, so it needs few large inserts; many
small ones fail with `TOO_MANY_PARTS`. A consumer group commits one offset per partition, meaning
"everything before this is done", and a rebalance can move a partition to another member at any time.
No transaction spans Kafka, ClickHouse and Postgres, and the process can die between any two writes.
Producers give every event an `id` and retry, so the topic can hold an event twice. A record that
never decodes, or a ClickHouse outage, must not stop a partition for good.

## Decision

**Commit after write, into an idempotent table.** A batch is flushed when it holds `BATCH_SIZE`
records or its first record has waited `BATCH_TIMEOUT`. A flush inserts the valid events in one
`INSERT`, produces dead letters, records the batch in the Postgres `batches` table and commits the
batch's offsets, in that order, and stops at the first step that fails; a crash or a failure before
the commit replays the batch, possibly cut differently. The `events` table is a
`ReplacingMergeTree(inserted_at)` sorted by `(event_type, occurred_at, event_id)`, so a replayed row
and its original collapse into one: at insert time when they share a block, otherwise when ClickHouse
merges their parts.

**Dead-letter after N attempts.** Every call a flush makes is bounded by `ATTEMPT_TIMEOUT`, and the
insert and the ledger write get `MAX_ATTEMPTS` attempts with exponential backoff capped at 10 s.
Records that do not decode go to the dead-letter topic with their batch; when the insert runs out of
attempts, the whole batch goes there, with headers naming the original topic, partition, offset and
the last error, and is committed. If shutdown cuts a failing insert short, nothing is dead-lettered
or committed. If the ledger write runs out of attempts, the pipeline stops without committing.

**Rebalances wait for the pending batch.** The client polls with `BlockRebalanceOnPoll` and calls
`AllowRebalance` only when no batch is pending, so every commit lands on partitions the member owns.

## Consequences

- No record is committed before it is in ClickHouse or in the dead-letter topic.
- Until merges run, a plain `count()` can see a replayed event twice; exact reads use `FINAL` or
  `uniqExact(event_id)`. An `id` reused with another `occurred_at` or `type` makes a second row.
- Replays duplicate what is not idempotent: the dead-letter topic can hold a record twice, and the
  ledger two rows for the same offsets.
- A failed commit is logged and not retried; its records are read again after the next restart or
  rebalance unless a later commit covers their partitions, and the table absorbs the replay. A failed
  commit during shutdown makes the service exit with an error.
- A rebalance can wait for `BATCH_TIMEOUT` plus a whole flush; the README gives the worst case, which
  must stay below the group's rebalance timeout.
- A ClickHouse outage longer than the retry budget moves traffic to the dead-letter topic; a Postgres
  outage stops the pipeline. One flush runs at a time per process.

## Alternatives considered

**Insert deduplication tokens.** ClickHouse drops a repeated insert only if the replay reproduces the
exact batch, and batch boundaries here depend on poll timing and partition ownership.

**Offsets stored with the data.** Exactly-once needs offsets and writes in one transaction, and
ClickHouse inserts are not transactional with Postgres.

**No dead-letter topic.** Retrying forever keeps order, but one poison record or a long outage stops
the partition and keeps rebalances blocked.
