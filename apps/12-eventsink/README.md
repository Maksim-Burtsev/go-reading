# eventsink

A Kafka-to-ClickHouse sink. It reads JSON events from a Kafka topic as a member of a consumer group,
writes them to ClickHouse in batches, records every flushed batch in Postgres and commits the batch's
offsets only after both writes succeeded. Records that are not valid events, and batches ClickHouse
keeps rejecting, go to a dead-letter topic. Delivery is at-least-once; the ClickHouse table collapses
replayed events. The service exposes `/health` and `/metrics` on one HTTP port.

The batching, commit and idempotency choices are recorded in
[ADR-001: batch strategy](docs/adr/001-batch-strategy.md).

## Architecture

```mermaid
flowchart LR
    topic[("Kafka topic<br/>events")] -->|poll| consumer["Consumer group<br/>franz-go, manual commits"]
    consumer --> batcher["Batcher<br/>BATCH_SIZE or BATCH_TIMEOUT"]
    batcher -->|valid events| insert["Insert with retry<br/>and backoff"]
    batcher -->|invalid JSON| dlq[("Kafka topic<br/>events.dlq")]
    insert -->|ok| clickhouse[("ClickHouse<br/>events, ReplacingMergeTree")]
    insert -->|MAX_ATTEMPTS exhausted| dlq
    clickhouse --> record["Record batch"]
    dlq --> record
    record --> postgres[("Postgres<br/>batches")]
    postgres --> commit["Commit offsets"]
    commit -.->|next poll| topic
    subgraph http ["HTTP :8012"]
        health["GET /health"]
        metrics["GET /metrics"]
    end
    health -.->|ping| clickhouse
    health -.->|ping| postgres
    health -.->|ping| topic
```

One goroutine does all of the pipeline work: poll, batch, flush, commit. A flush runs these steps in
order and stops at the first one that fails, leaving the batch uncommitted:

1. Insert the valid events into ClickHouse in one `INSERT` block, retrying with capped exponential
   backoff up to `MAX_ATTEMPTS` times.
2. Produce invalid records, and the whole batch if the insert ran out of attempts, to the DLQ topic
   with headers that point at the original topic, partition, offset and error.
3. Insert one row into the Postgres `batches` table: status, record and dead-letter counts, insert
   duration, and the first and last offset taken from each partition.
4. Commit the batch's offsets.

On `SIGTERM` the service stops polling, flushes and commits the pending batch within
`SHUTDOWN_TIMEOUT`, stops the HTTP server, leaves the consumer group and closes ClickHouse and
Postgres.

## Run

The compose file starts Redpanda, ClickHouse and Postgres on host ports chosen not to clash with the
other applications in this repository. The `eventsink` service itself is behind the `app` profile.

Full stack, with the service built from the [Dockerfile](Dockerfile):

```sh
docker compose -f apps/12-eventsink/docker-compose.yml -p gr-12 --profile app up -d --build --wait
```

Infrastructure only, with the service on the host (the env defaults point at the compose ports):

```sh
docker compose -f apps/12-eventsink/docker-compose.yml -p gr-12 up -d --wait
go run ./apps/12-eventsink/cmd/eventsink
```

Produce events and look at the result:

```sh
compose="docker compose -f apps/12-eventsink/docker-compose.yml -p gr-12"
printf '%s\n' \
  '{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:00Z","payload":{"total":42}}' \
  '{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:00Z","payload":{"total":42}}' \
  'not json' | $compose exec -T redpanda rpk topic produce events -X brokers=localhost:9092

$compose exec clickhouse clickhouse-client --user eventsink --password eventsink -d eventsink \
  --query 'SELECT * FROM events FINAL'
$compose exec postgres psql -U eventsink -c 'SELECT * FROM batches ORDER BY id DESC LIMIT 5'
$compose exec redpanda rpk topic consume events.dlq -n 1 -f '%v %h{%k=%v }\n' -X brokers=localhost:9092
$compose exec redpanda rpk group describe eventsink -X brokers=localhost:9092
curl -s localhost:8012/health
curl -s localhost:8012/metrics | grep ^eventsink_

$compose --profile app down -v
```

Tests:

```sh
go test -race ./apps/12-eventsink/...
go test -race -tags integration ./apps/12-eventsink/...   # needs Docker
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `ADDR` | `:8012` | Listen address of the HTTP server with `/health` and `/metrics`. |
| `KAFKA_BROKERS` | `localhost:19012` | Comma-separated seed brokers. |
| `KAFKA_GROUP` | `eventsink` | Consumer group ID. |
| `KAFKA_TOPIC` | `events` | Input topic. |
| `KAFKA_DLQ_TOPIC` | `events.dlq` | Dead-letter topic; must differ from `KAFKA_TOPIC`. |
| `CLICKHOUSE_URL` | `clickhouse://eventsink:eventsink@localhost:9012/eventsink?dial_timeout=5s&compress=lz4` | ClickHouse native-protocol DSN. |
| `DATABASE_URL` | `postgres://eventsink:eventsink@localhost:5412/eventsink?sslmode=disable` | Postgres connection string for the batch ledger. |
| `BATCH_SIZE` | `1000` | Records per batch; a full batch is flushed at once. |
| `BATCH_TIMEOUT` | `2s` | Maximum time the first record of a batch waits before the batch is flushed. |
| `MAX_ATTEMPTS` | `5` | Attempts for the ClickHouse insert and for the ledger write. |
| `RETRY_BACKOFF` | `200ms` | Wait before the second attempt; doubled for every further attempt, capped at 10s. |
| `SHUTDOWN_TIMEOUT` | `15s` | How long a flush may keep running after `SIGTERM`, and the HTTP shutdown deadline. |

The consumer holds rebalances back while a batch is in flight, so `BATCH_TIMEOUT` plus the worst-case
retry time must stay below the group's rebalance timeout (60 s by default). The compose service sets
`stop_grace_period` above `SHUTDOWN_TIMEOUT` so Docker does not kill a flush in progress.

Topics are expected to exist. The client asks for automatic creation, which Redpanda in dev mode (and
any broker with `auto.create.topics.enable`) honours.

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `eventsink_records_consumed_total` | counter | |
| `eventsink_batches_flushed_total` | counter | `outcome`: `inserted`, `dead_lettered` |
| `eventsink_dead_letters_total` | counter | `reason`: `invalid`, `insert_failed` |
| `eventsink_batch_size_records` | histogram | |
| `eventsink_flush_duration_seconds` | histogram | |

`/health` pings ClickHouse, Postgres and a Kafka broker concurrently with a 2 s deadline and answers
`200` or `503` with the state of each dependency:

```json
{"status":"unavailable","dependencies":{"clickhouse":"ping clickhouse: dial tcp [::1]:9012: connect: connection refused","kafka":"ok","postgres":"ok"}}
```
