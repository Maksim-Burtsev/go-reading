# go-reading

Twelve small Go services, written the way good production Go is written, for practice in reading
and reviewing Go. There are no teaching shortcuts and no comments that explain the language: this is
the code you will meet at work.

## How to use it

Take the apps in order. Each one is a read, then a review.

**Read.** Open the app in [merl](https://github.com/Maksim-Burtsev/merl): `merl apps/01-cli-wordfreq`
(`merl --tutor` teaches the keys in ten minutes). A Go service starts in `cmd/<name>/main.go`: `main`
calls `run`, and `run` wires up what lives in `internal/`. The app's `READING.md` shows how to run it
and asks six to eight questions. Answer each one from the code before you open its answer; in order,
they take you from `main` to the core of the app.

**Review.** Each app has a review branch: one PR-sized change to that app, with defects planted in
it.

```sh
merl --review=review/001-keep-going
```

switches to the branch and walks its diff against master, `c` / `C` for the next and previous hunk.
`PR.md`, the first file, says what the change is for. Write down every problem you would block the
PR on, with file and line. CI is green on these branches, so the linters and tests found nothing:
what is left is what only a reader sees. Then `git switch master`.

**Grade.** Tell Claude, in this repository, "verdict for 001: …". It checks your list against the
answer key, explains each defect you missed and why each false alarm is fine, and adds a row to
[`review/LOG.md`](review/LOG.md). The key is `SOLUTION.md` on `review/001-keep-going-solution`;
leave it closed until you have given a verdict.

## Apps

1. [cli-wordfreq](apps/01-cli-wordfreq/READING.md) — cobra CLI: word frequency and dedupe, bounded
   errgroup fan-out, tabwriter. Review: `review/001-keep-going`.
2. [http-notes](apps/02-http-notes/READING.md) — net/http JSON CRUD: ServeMux `{id}` routing,
   middleware chain, validator, graceful shutdown. Review: `review/002-tag-filter`.
3. [ratelimiter](apps/03-ratelimiter/READING.md) — token bucket and sliding window behind one
   interface, fake clock, `X-RateLimit-*` middleware. Review: `review/003-delay-instead-of-reject`.
4. [worker-pool](apps/04-worker-pool/READING.md) — webhook dispatcher: bounded queue, errgroup
   workers, backoff with jitter, graceful drain, goleak. Review: `review/004-per-host-limit`.
5. [lru-cache](apps/05-lru-cache/READING.md) — generic LRU with TTL and OnEvict, benchmarks,
   read-through caching proxy with singleflight. Review: `review/005-cache-snapshots`.
6. [pg-service](apps/06-pg-service/READING.md) — chi + pgx + sqlc + goose: order transaction,
   keyset pagination, `PgError` → 409, testcontainers. Review: `review/006-order-idempotency-keys`.
7. [kafka-consumer](apps/07-kafka-consumer/READING.md) — franz-go consumer group: size/time
   batching, commit after write, retries, dead-letter topic. Review: `review/007-pipelined-flush`.
8. [grpc-service](apps/08-grpc-service/READING.md) — buf-generated gRPC inventory: server
   streaming, interceptors, deadlines, status codes, bufconn. Review: `review/008-release-reservation`.
9. [cron-worker](apps/09-cron-worker/READING.md) — robfig/cron jobs under Postgres advisory locks,
   duration metrics, fake clock. Review: `review/009-intraday-rollup`.
10. [tui-app](apps/10-tui-app/READING.md) — bubbletea + lipgloss: two-pane task list with a filter,
    `Update` tested as a pure function. Review: `review/010-autosave`.
11. [clickhouse-sink](apps/11-clickhouse-sink/READING.md) — HTTP ingest into ClickHouse batches,
    429 backpressure, Prometheus metrics, health check. Review: `review/011-buffer-byte-budget`.
12. [eventsink](apps/12-eventsink/READING.md) — Kafka → ClickHouse batches with a Postgres batch
    ledger, idempotent replay, distroless image, ADR-001. Review: `review/012-dead-letter-rejected-events`.

How the questions and the reviews are written, and how new ones get added:
[AUTHORING.md](AUTHORING.md).

## Checks

```sh
make lint              # gofmt, go vet, golangci-lint
make test              # go test -race
make test-integration  # plus the //go:build integration tests, needs Docker
```

CI runs `make lint` and `make test` on every push.
