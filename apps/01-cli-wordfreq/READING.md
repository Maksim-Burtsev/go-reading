# 01-cli-wordfreq

A cobra CLI that counts Unicode word frequencies across files and stdin concurrently, and removes duplicate lines from a stream.

## Run

```sh
go run ./apps/01-cli-wordfreq/cmd/wordfreq count --top 10 README.md CLAUDE.md; go run ./apps/01-cli-wordfreq/cmd/wordfreq dedupe --count CLAUDE.md
```

## Where to start

1. `cmd/wordfreq/main.go:run` — the whole process boundary: args, env, stdin/stdout/stderr come in as parameters and are handed to cobra.
2. `internal/cli/root.go:NewRootCommand` — the command tree, the env-backed `--log-level` default and the logger built in `setUp`.
3. `internal/cli/count.go:countCommand.run` — flag validation, the call into the domain package and the choice between table and JSON output.
4. `internal/wordfreq/count.go:CountAll` — bounded concurrent fan-out over inputs with `errgroup`, merged under a mutex.
5. `internal/wordfreq/scan.go:ScanWords` — the tokenizer, written as a `bufio.SplitFunc` that works on arbitrary chunk boundaries.

## Data flow

1. `main` builds a context canceled by SIGINT/SIGTERM and calls `run` with `os.Args`, `os.Getenv` and the standard streams.
2. `run` builds the cobra tree, points its args/in/out/err at its own parameters and calls `ExecuteContext`; cobra parses flags and picks `count` or `dedupe`.
3. The root `setUp` hook parses `--log-level` (default from `WORDFREQ_LOG_LEVEL`) into a JSON `slog` logger on stderr.
4. `count`: `input.Names` turns operands into names (none means `-`), `wordfreq.CountAll` runs one goroutine per name, at most `--jobs` at a time.
5. Each goroutine opens its name through `input.Opener` (a context-bound reader), scans words with `ScanWords`, case-folds them and counts into its own map, then merges it into the total.
6. `Counts.Top` sorts by count descending, word ascending, and cuts to `--top`; the report is written with `tabwriter` or `encoding/json`.
7. `dedupe`: inputs are read one after another into a single `dedupe.Set`; first-seen lines go straight to a buffered stdout, or with `--count` all lines are printed at the end.
8. Errors are wrapped on the way up (`count: read a.txt: ...`), cobra returns them silently, and `main` prints once and exits 1.

## Go specifics here

1. `cmd/wordfreq/main.go:18` — `context.AfterFunc(ctx, stop)`
   <details><summary>Explanation</summary>

   `signal.NotifyContext` swallows SIGINT for as long as it is registered, so a process blocked on a
   stdin read would ignore every Ctrl-C. `AfterFunc` runs `stop` as soon as the first signal cancels
   `ctx`; that unregisters the handler and a second Ctrl-C gets the default behavior and kills the
   process. `stop()` is also called by hand on line 20 because `os.Exit` does not run deferred calls.
   </details>

2. `internal/wordfreq/count.go:70` — `utf8.RuneCountInString(word) < minLen`
   <details><summary>Explanation</summary>

   A Go `string` is a read-only byte slice, and `len` returns bytes, not characters. Every Cyrillic
   letter is two bytes in UTF-8, so `len("ёж")` is 4. `RuneCountInString` counts code points, which is
   what Python's `len` does on a `str`. Indexing a string yields bytes; `for range` over it yields runes.
   </details>

3. `internal/wordfreq/count.go:104` — `g.Go(func() error {` using `name`
   <details><summary>Explanation</summary>

   Since Go 1.22 every loop iteration has its own `name` variable, so each closure captures a
   different value. In Python a closure in a loop sees the last value (late binding); in Go before
   1.22 this was the same bug. `g.Go` blocks once `SetLimit` goroutines are running, which is what
   bounds concurrency. `errgroup.WithContext` on line 98 shadows `ctx` with one that is canceled on the
   first returned error.
   </details>

4. `internal/wordfreq/count.go:121` — `(_ Counts, err error)` with `defer func() { err = ... }()`
   <details><summary>Explanation</summary>

   Named results are ordinary variables that `return` assigns before deferred calls run, so a
   deferred closure can still change what the caller receives. Here it joins the `Close` error into
   `err`; `errors.Join(nil, nil)` is `nil`. Results must be all named or all unnamed, hence the `_`.
   </details>

5. `internal/input/input.go:50` — `type readCloser struct { io.Reader; io.Closer }`
   <details><summary>Explanation</summary>

   Embedding promotes the methods of the embedded values: `Read` comes from the context-bound reader,
   `Close` from the `*os.File`, and the struct satisfies `io.ReadCloser` without saying so. The same
   structural typing lets `input.Opener` satisfy `wordfreq.Opener` and `dedupe.Opener`, interfaces
   declared by the packages that consume them, which never import each other's types for it.
   </details>

## Questions

1. `count a.txt a.txt` works, but `count a.txt - -` fails before reading anything. Why the difference?
   <details><summary>Answer</summary>

   A file path can be opened twice, giving two independent readers. Standard input is one shared
   stream: two goroutines would read it concurrently (a data race on the reader) and split its
   contents between them. `input.Names` rejects a repeated `-` with `ErrStdinRepeated` up front.
   </details>

2. Inputs finish in random order and Go map iteration order is randomized. Why is the output still deterministic?
   <details><summary>Answer</summary>

   Merging is addition, which does not depend on order, so the final `Counts` is the same whichever
   goroutine finishes first. `Counts.Top` then imposes a total order: `cmp.Or` compares by count
   descending and falls back to the word ascending when counts tie.
   </details>

3. With `--jobs 4`, one of ten files does not exist. What happens to the others and what does the user see?
   <details><summary>Answer</summary>

   The failing goroutine returns the open error, and `errgroup` cancels the shared context. Readers
   from `input.Opener` check the context before and after every `Read`, so inputs in flight stop with
   `context.Canceled` at their next read, and the ones started later fail on their first read.
   `g.Wait` returns only the first error, so the user sees `count: open x.txt: no such file or directory`
   and exit status 1, with no partial table.
   </details>

4. Why is `cases.Fold()` created inside `Count` instead of once at package level?
   <details><summary>Answer</summary>

   A `cases.Caser` keeps internal state and must not be shared between goroutines, and `Count` runs
   in several goroutines at once. A package-level caser would be a data race as well as global
   mutable state. Creating one per call is cheap.
   </details>

5. `slow-producer | wordfreq dedupe` prints nothing for a long time, although new distinct lines keep arriving. Why, and where would you change it?
   <details><summary>Answer</summary>

   `onFirst` writes into a `bufio.Writer` in `internal/cli/dedupe.go`, which only reaches stdout when
   its 4 KiB buffer fills or at the final `Flush`. Flushing after every line (for example behind a
   `--line-buffered` flag) would make output immediate at the cost of one write syscall per line.
   </details>
