# 12 · eventsink

A Kafka consumer (franz-go, consumer group) that reads JSON events from a topic, writes them to ClickHouse in
batches of up to 1000 records or every 2 seconds, records each batch in a Postgres ledger, and only then commits
the batch's offsets. Records that do not decode, and batches ClickHouse keeps refusing, go to a dead-letter topic,
and a batch read twice after a crash still leaves one row per event, because the table deduplicates on the
event's key. `/health` and `/metrics` are served on port 8012.

## Run it

```sh
export COMPOSE_FILE=apps/12-eventsink/docker-compose.yml COMPOSE_PROJECT_NAME=gr-12
docker compose up -d --wait
docker compose exec redpanda rpk topic create events events.dlq -X brokers=localhost:9092
go run ./apps/12-eventsink/cmd/eventsink
```

In a second terminal, from the repository root:

```sh
export COMPOSE_FILE=apps/12-eventsink/docker-compose.yml COMPOSE_PROJECT_NAME=gr-12
printf '%s\n' '{"id":"e-1","type":"order.created","occurred_at":"2026-09-21T10:00:00Z","payload":{"total":42}}' 'not json' |
  docker compose exec -T redpanda rpk topic produce events -X brokers=localhost:9092
docker compose exec clickhouse clickhouse-client --user eventsink --password eventsink -d eventsink \
  --query 'SELECT * FROM events FINAL'
docker compose exec postgres psql -U eventsink -c 'SELECT * FROM batches ORDER BY id DESC LIMIT 5'
docker compose exec redpanda rpk topic consume events.dlq -n 1 -X brokers=localhost:9092
curl -s localhost:8012/health
```

Stop the service and start it again as a new consumer group, `KAFKA_GROUP=replay go run ./apps/12-eventsink/cmd/eventsink`:
it reads the topic from the start, `batches` gets new rows for the same offsets, and
`SELECT count() FROM events FINAL` does not change. `docker compose down -v` removes the stack.

## Questions

Answer from the code first, then open the answer.

1. The HTTP listener fails while the pipeline is running. How does the pipeline find out, and how does the
   process end?

   <details><summary>Answer</summary>

   `run` wraps its context with `context.WithCancel`, and the goroutine running `srv.Serve` calls `cancel` when
   `Serve` returns. `Pipeline.Run` loops only while `ctx.Err() == nil`, so it stops polling, flushes and commits
   the pending batch, and returns; `run` then shuts the server down, receives the `Serve` error (anything but
   `http.ErrServerClosed` counts) and returns it, and the process exits with status 1 for the orchestrator to
   restart. Without that `cancel`, the consumer would keep running with no `/health` and no `/metrics`. Rule: when
   long-running parts of a process share a lifetime, a failure in any of them must cancel the shared context;
   `errgroup.WithContext` packages the same pattern.

   </details>

2. `pipeline.ClientOptions` gives `main` the kgo options, and `main` only appends the seed brokers. Which of those
   options, if someone dropped it, would break the delivery guarantee, and how?

   <details><summary>Answer</summary>

   Each `kgo.Opt` is a functional option, and `append(...)...` spreads the slice into `kgo.NewClient`'s variadic
   parameter. Without `DisableAutoCommit`, kgo commits in the background every 5 s whatever earlier polls returned;
   a batch is often built from several polls, so offsets of records not yet in ClickHouse can be committed, and a
   crash then loses those records. Without `BlockRebalanceOnPoll`, a rebalance can land between a poll and
   `CommitRecords`, and the commit then lands on partitions another member now owns, duplicating its work or
   rewinding its progress. Keeping the options in the package that depends on them stops `main` from building a
   client that quietly breaks commit-after-write. Rule: the component whose correctness depends on a client's
   configuration should own that configuration.

   </details>

3. `/health` pings ClickHouse, Postgres and Kafka, each in its own goroutine. What stops a slow dependency from
   making the handler slow, or leaving goroutines behind, and why can `*pgxpool.Pool` and `*kgo.Client` be passed
   as a `Pinger` without adapters?

   <details><summary>Answer</summary>

   All pings share one context with a 2 s timeout, and the results channel is buffered with one slot per
   dependency, so every goroutine can deliver its result and exit, and the handler reads exactly `len(deps)`
   results. Since Go 1.22 each loop iteration has its own `name` and `dep`, so every goroutine pings its own
   dependency; before that, they could all have seen the last pair. The deadline only bounds a `Ping` that honours
   its context; one that ignores it would still hold the handler. Go interfaces are satisfied implicitly, much like
   a `typing.Protocol`: any type with `Ping(context.Context) error` is a `Pinger`, so the interface can live in
   `server`, where it is used. Rule: fan out with a result channel sized to the number of senders and one shared
   deadline, and declare the interface on the consumer's side.

   </details>

4. Three records arrive and the topic goes quiet, with `BATCH_SIZE` at 1000. What gets them written after
   `BATCH_TIMEOUT` without a timer goroutine, and what would happen if `poll` passed `ctx` straight to
   `PollRecords`?

   <details><summary>Answer</summary>

   When the first record of a batch arrives, `poll` sets `p.deadline` to now plus `BATCH_TIMEOUT`, and while the
   batch is not empty `pollContext` gives `PollRecords` a context with that deadline, so the call returns at the
   deadline even if nothing new comes and `Run` flushes. With plain `ctx`, `PollRecords` would block until more
   records came: on a quiet topic the three would wait indefinitely, unwritten and uncommitted, with rebalances
   held back. The limit passed to `PollRecords`, `BATCH_SIZE` minus what is pending, keeps a batch from
   overshooting; it is always at least 1 here, which matters because kgo reads 0 as "no limit". Rule: when a loop
   blocks on a call but owes work at a certain time, bound the call with a context deadline set to that time.

   </details>

5. Why does `Run` call `AllowRebalance` only when no batch is pending, and what does that mean for
   `BATCH_TIMEOUT` and the retry settings?

   <details><summary>Answer</summary>

   With `BlockRebalanceOnPoll`, a poll that returns records holds back any rebalance until `AllowRebalance` is
   called. If partitions could move while a batch was pending, its `CommitRecords` could commit offsets for
   partitions another member now reads, and both would process the same records. Allowing rebalances only between
   batches keeps every commit on partitions this member owns. The price: a rebalance waits for the whole batch,
   its timeout plus every attempt and backoff of the flush, and that must stay under the group's rebalance
   timeout, or the group drops the member and its partitions are replayed elsewhere. `ATTEMPT_TIMEOUT` bounds
   each call, which caps the wait at 44 s with the defaults, against kgo's 60 s. Rule: blocking rebalances buys
   safe commits with slower group changes, so the time to process one batch needs a hard bound.

   </details>

6. SIGTERM arrives in the middle of an insert. Why is the insert not cut off, and what stops it from running for
   ever?

   <details><summary>Answer</summary>

   `Run` builds `flushCtx` from `context.WithoutCancel(ctx)`, so the signal does not cancel flushes, and wraps it
   in its own `WithCancel`. `context.AfterFunc(ctx, f)` runs `f` in a new goroutine once `ctx` is done, and `f`
   arms `time.AfterFunc(ShutdownTimeout, cancel)`, so the flush in progress and the final flush of the pending
   batch share one budget that starts at the signal. When it runs out, every call under `flushCtx` fails (the
   ClickHouse driver closes its connection mid-`Send`), and the batch stays uncommitted for the next group member.
   `defer stop()` unregisters the callback when `Run` returns before any signal. Rule: detach in-flight work from
   the shutdown signal with `WithoutCancel`, then give it back a deadline counted from the signal.

   </details>

7. `retry` returned an error that wraps `errExhausted`, yet `flush` does not dead-letter the batch when
   `ctx.Err() != nil`. Why, and how can one error match both `errExhausted` and ClickHouse's error?

   <details><summary>Answer</summary>

   `fmt.Errorf` with two `%w` verbs wraps both errors and `errors.Is` searches both branches, so `flush` branches
   on the sentinel while the log and the `dlq-error` header keep ClickHouse's own message, much like matching
   inside a Python `ExceptionGroup`. The check reads the flush's context, not the per-call one `attempt` derives:
   an attempt that used up its own `ATTEMPT_TIMEOUT` is an ordinary failure, but a flush whose shutdown budget
   ran out says nothing about the batch. Dead-lettering then would send healthy events to the DLQ and commit them
   only because the service was stopping; returning the error leaves the batch uncommitted for the next member.
   Rule: classify errors with sentinels and `errors.Is`, and never turn a cancellation into a permanent outcome
   such as a dead letter, a delete or a "failed" status.

   </details>

8. The process is killed after `InsertEvents` succeeded and before `CommitRecords` ran. What happens after the
   restart in ClickHouse, in the `batches` table and in the dead-letter topic?

   <details><summary>Answer</summary>

   A Kafka commit stores one position per partition, the next offset to read, so the group resumes from the last
   successful commit and reads the whole batch again, possibly cut into different batches. ClickHouse receives the
   same rows a second time; the table is a `ReplacingMergeTree` whose sorting key `(event_type, occurred_at,
   event_id)` identifies an event, so copies collapse at insert time when they share a block and otherwise in a
   background merge, and `SELECT ... FINAL` returns one row per event right away. `batches` gets new rows for the
   same offsets, because it is a log of flushes, and invalid records reach the DLQ a second time. Rule:
   at-least-once delivery into an idempotent write gives exactly-once results only where the write is idempotent;
   everywhere else, expect duplicates.

   </details>
