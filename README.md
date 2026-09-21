# go-reading

Small production-grade Go applications, written to be read and reviewed.

The reader is a backend engineer fluent in Python who is learning to read Go. The code is what
good production Go looks like: no teaching shortcuts and no comments that explain the language.
Each application lives in `apps/NN-name/` with its own `cmd/` and `internal/`, under one `go.mod`.

## How to read

Read with [merl](https://github.com/Maksim-Burtsev/merl):

```sh
brew install maksim-burtsev/tap/merl
merl apps/01-cli-wordfreq
```

Start at `cmd/*/main.go`, follow `main` into `run`, and from there into `internal/`. `d` goes to a
definition, `u` lists usages, `[` comes back, `s` searches the project, `D` lists its symbols.

Run an application:

```sh
make run-01-cli-wordfreq ARGS="..."
```

## Applications

Each application has a `READING.md`: where to start, the data flow, Go-specific spots and questions.

- [01-cli-wordfreq](apps/01-cli-wordfreq/READING.md) — cobra CLI: word frequency and dedupe, bounded errgroup fan-out, tabwriter.
- [02-http-notes](apps/02-http-notes/READING.md) — net/http JSON CRUD: ServeMux `{id}` routing, middleware chain, validator, graceful shutdown.
- [03-ratelimiter](apps/03-ratelimiter/READING.md) — token bucket and sliding window behind one interface, fake clock, `X-RateLimit-*` middleware.
- [04-worker-pool](apps/04-worker-pool/READING.md) — webhook dispatcher: bounded queue, errgroup workers, backoff with jitter, graceful drain, goleak.
- [05-lru-cache](apps/05-lru-cache/READING.md) — generic LRU with TTL and OnEvict, benchmarks, read-through caching proxy with singleflight.
- [06-pg-service](apps/06-pg-service/READING.md) — chi + pgx + sqlc + goose: order transaction, keyset pagination, `PgError` → 409, testcontainers.

## Review branches

Each review is a pull-request-shaped branch off `master` with issues planted in it.

1. `merl --review=review/NNN-topic` fetches the branch, switches to it and shows its diff against
   `master`. `c` / `C` walk the hunks.
2. Write down the verdict: every issue found, with file and line.
3. Only then open `review/NNN-topic-solution`, which holds the answers.
4. Add a row to [`review/LOG.md`](review/LOG.md) on `master`: planted, found, false positives,
   minutes spent.

## Checks

```sh
make lint              # gofmt, go vet, golangci-lint
make test              # go test -race
make test-integration  # plus //go:build integration tests, needs Docker
```

CI runs `make lint` and `make test` on every push.
