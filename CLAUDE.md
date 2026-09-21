# go-reading

A study repository: small production-grade Go applications for reading and review.
The reader is a senior Python backend engineer learning to read Go. The code must look
like good production code: no teaching simplifications and no explanatory comments.

Everything in this repository is in English: code, comments, commit messages, docs.

## Tooling
- Go 1.27. A single go.mod at the root; each application is its own directory
  `apps/NN-name/` with its own `cmd/` and `internal/`.
- Before every commit: `make lint test` (`gofmt -l .`, `go vet ./...`, `golangci-lint run`,
  `go test -race ./...`). Everything must be clean. Do not commit if anything is red.
- Linter: `.golangci.yml` at the root, with errcheck, govet, staticcheck, gocritic,
  errorlint, bodyclose, noctx, sqlclosecheck, contextcheck, gosec, revive, unused enabled.

## Style
- Uber Go Style Guide. Google Go Style Guide best practices.
- Service layout per Mat Ryer: `func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error`;
  `main` only calls `run` and does `os.Exit(1)` on error.
- Errors: only `fmt.Errorf("...: %w", err)`, `errors.Is`/`errors.As`, sentinel errors
  as `var ErrX = errors.New(...)`. No panics outside `main`/`init`.
- `context.Context` is the first argument of every function doing I/O, timeouts or cancellation.
- Logging: `log/slog`, JSON handler, `InfoContext`/`ErrorContext`.
- Interfaces are small and declared by the consumer, not the producer.
- Tests are table-driven, `testify/require`, `t.Parallel()` where safe;
  integration tests use `testcontainers-go` behind `//go:build integration`.
- No global variables with state, no `init()`.
- Config via env: `caarlos0/env/v11`.
- Go 1.22+ idioms: `for range n`, `min`/`max`, `slices`/`maps`, `net/http` routing
  with methods and `{id}`, `r.PathValue`.

## Review branches
- `review/NNN-topic` is the PR branch to read. `review/NNN-topic-solution` holds the answers.
  Never reveal the contents of a solution branch until the reader has stated their verdict.
- `review/LOG.md` on master is the results log.
