# go-reading

A study repository: small production-grade Go applications for reading and review.
The reader is a senior Python backend engineer learning to read Go. The code must look
like good production code: no teaching simplifications and no explanatory comments.

Everything in this repository is in English: code, comments, commit messages, docs.

## Tooling
- Go 1.27. A single go.mod at the root; each application is its own directory
  `apps/NN-name/` with its own `cmd/` and `internal/`.
- Before every commit that changes code: `make lint test` (`gofmt -l .`, `go vet ./...`,
  `golangci-lint run`, `go test -race ./...`). Everything must be clean. Do not commit if anything
  is red.
- Untracked files in the checkout are the reader's own exercises: leave them as they are, and when
  they break `make lint`, run the checks in a clean worktree.
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

## Reading and reviews
- The reader's loop is in README.md. Writing a READING.md or a new review branch: follow
  AUTHORING.md.
- `review/NNN-slug` is a PR with planted defects; its answer key is `SOLUTION.md` on
  `review/NNN-slug-solution`. The key stays sealed: until the reader gives a verdict for that
  review, nothing from the solution branch reaches them, hints included.
- Grading a verdict ("verdict for 003: …", in any language):
  1. `git fetch origin`, then read `git show origin/review/003-<slug>-solution:SOLUTION.md`.
  2. Match each finding to a defect by its "Found if" line. A finding that matches nothing is a
     false positive, unless it is listed under "Also acceptable" or is a real problem the key
     lacks: credit it, and add a missing one to SOLUTION.md on the solution branch.
  3. Answer in the reader's language: what they found; each missed defect with its tell and rule;
     each false positive with why the code is fine.
  4. Append a row to `review/LOG.md` on master (ask for the minutes if they were not given),
     commit, push.
