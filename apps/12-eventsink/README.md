# eventsink

A Kafka-to-ClickHouse sink. It reads JSON events from a Kafka topic as a member of a consumer group,
writes them to ClickHouse in batches, records every flushed batch in Postgres and commits the batch's
offsets only after both writes succeeded. Records that are not valid events, and events ClickHouse
keeps rejecting, go to a dead-letter topic. Delivery is at-least-once; the ClickHouse table collapses
replayed events. The service exposes `/health` and `/metrics` on one HTTP port.

The batching, commit and idempotency choices are recorded in
[ADR-001: batch strategy](docs/adr/001-batch-strategy.md).

## Architecture

```mermaid
flowchart LR
    subgraph kafka ["Kafka"]
        events[("events")]
        dlq[("events.dlq")]
    end
    subgraph eventsink ["eventsink"]
        pipeline["Pipeline<br/>poll, batch, flush"]
        http["HTTP :8012<br/>/health, /metrics"]
    end
    clickhouse[("ClickHouse<br/>events")]
    postgres[("Postgres<br/>batches")]

    events -->|poll| pipeline
    pipeline -->|"1: insert valid events"| clickhouse
    pipeline -->|"2: dead letters"| dlq
    pipeline -->|"3: record the batch"| postgres
    pipeline -->|"4: commit offsets"| kafka
    http -.->|ping| clickhouse
    http -.->|ping| postgres
    http -.->|ping a broker| kafka
```

One goroutine runs the pipeline, and a flush takes the numbered steps in order. It stops at the first
step that fails and leaves the batch uncommitted, so the batch is read again after a restart or a
rebalance. The insert and the ledger write are retried with capped exponential backoff. When the
insert runs out of attempts, the batch is split in halves and each half is inserted the same way, down
to single events. Dead letters are the records that do not decode, plus the events ClickHouse refuses
even on their own, and carry headers with the original topic, partition, offset and error. The `batches` row holds the
status, the record and dead-letter counts, the insert duration, and the first and last offset taken
from each partition.

On `SIGTERM` the service stops polling, flushes and commits the pending batch within
`SHUTDOWN_TIMEOUT`, stops the HTTP server, leaves the consumer group and closes ClickHouse and
Postgres.

## Run

The compose file starts Redpanda, ClickHouse and Postgres on host ports chosen not to clash with the
other applications in this repository. The service does not create topics, so create them once the
broker is up:

```sh
export COMPOSE_FILE=apps/12-eventsink/docker-compose.yml COMPOSE_PROJECT_NAME=gr-12
docker compose up -d --wait
docker compose exec redpanda rpk topic create events events.dlq -X brokers=localhost:9092
```

Then run the service on the host (the env defaults point at the compose ports):

```sh
go run ./apps/12-eventsink/cmd/eventsink
```

or in a container built from the [Dockerfile](Dockerfile), behind the `app` profile:

```sh
docker compose --profile app up -d --build --wait
```

Produce events and look at the result:

```sh
printf '%s\n' \
  '{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:00Z","payload":{"total":42}}' \
  '{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:00Z","payload":{"total":42}}' \
  'not json' | docker compose exec -T redpanda rpk topic produce events -X brokers=localhost:9092

docker compose exec clickhouse clickhouse-client --user eventsink --password eventsink -d eventsink \
  --query 'SELECT * FROM events FINAL'
docker compose exec postgres psql -U eventsink -c 'SELECT * FROM batches ORDER BY id DESC LIMIT 5'
docker compose exec redpanda rpk topic consume events.dlq -n 1 -f '%v %h{%k=%v }\n' -X brokers=localhost:9092
docker compose exec redpanda rpk group describe eventsink -X brokers=localhost:9092
curl -s localhost:8012/health
curl -s localhost:8012/metrics | grep ^eventsink_

docker compose --profile app down -v
```

Tests:

```sh
go test -race ./apps/12-eventsink/...
DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}')" \
  go test -race -tags integration ./apps/12-eventsink/...
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `ADDR` | `:8012` | Listen address of the HTTP server with `/health` and `/metrics`. |
| `KAFKA_BROKERS` | `localhost:19012` | Comma-separated seed brokers. |
| `KAFKA_GROUP` | `eventsink` | Consumer group ID. |
| `KAFKA_TOPIC` | `events` | Input topic; must exist. |
| `KAFKA_DLQ_TOPIC` | `events.dlq` | Dead-letter topic; must exist and differ from `KAFKA_TOPIC`. |
| `CLICKHOUSE_URL` | `clickhouse://eventsink:eventsink@localhost:9012/eventsink?dial_timeout=5s&compress=lz4` | ClickHouse native-protocol DSN. |
| `DATABASE_URL` | `postgres://eventsink:eventsink@localhost:5412/eventsink?sslmode=disable` | Postgres connection string for the batch ledger. |
| `BATCH_SIZE` | `1000` | Records per batch; a full batch is flushed at once. |
| `BATCH_TIMEOUT` | `2s` | Maximum time the first record of a batch waits before the batch is flushed. |
| `MAX_ATTEMPTS` | `5` | Attempts for the ClickHouse insert and for the ledger write. |
| `RETRY_BACKOFF` | `200ms` | Wait before the second attempt; doubled for every further attempt, capped at 10s. |
| `ATTEMPT_TIMEOUT` | `3s` | Deadline of each insert and ledger attempt, of the dead-letter produce and of the offset commit. |
| `SHUTDOWN_TIMEOUT` | `15s` | How long a flush may keep running after `SIGTERM`, and the HTTP shutdown deadline. |

The consumer holds rebalances back while a batch is pending, so a rebalance can wait for
`BATCH_TIMEOUT` plus a whole flush. A flush takes at most `MAX_ATTEMPTS` × `ATTEMPT_TIMEOUT` plus the
backoff between attempts, once for the insert and once for the ledger write, plus `ATTEMPT_TIMEOUT`
each for the dead-letter produce and the commit. With the defaults that is 2 s + 2 × (5 × 3 s + 3 s) +
2 × 3 s = 44 s, below the 60 s rebalance timeout kgo uses by default. Settings that push the sum past
it let the group drop the member in the middle of a batch; the batch is then read again by the member
that takes over its partitions. The compose service sets `stop_grace_period` above
`SHUTDOWN_TIMEOUT` so Docker does not kill a flush in progress.

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `eventsink_records_consumed_total` | counter | |
| `eventsink_batches_flushed_total` | counter | `outcome`: `inserted`, `partially_dead_lettered`, `dead_lettered` |
| `eventsink_dead_letters_total` | counter | `reason`: `invalid`, `insert_failed` |
| `eventsink_rejected_events` | gauge | |
| `eventsink_batch_size_records` | histogram | |
| `eventsink_flush_duration_seconds` | histogram | |

`/health` pings ClickHouse, Postgres and a Kafka broker concurrently with a 2 s deadline and answers
`200` or `503` with the state of each dependency:

```json
{"status":"unavailable","dependencies":{"clickhouse":"ping clickhouse: dial tcp [::1]:9012: connect: connection refused","kafka":"ok","postgres":"ok"}}
```
