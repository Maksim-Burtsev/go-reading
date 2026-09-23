# 01 · cli-wordfreq

`wordfreq count` prints the most frequent words across files and standard input, as a table or as
JSON; `wordfreq dedupe` prints every distinct line once, in the order first seen, optionally with
its number of occurrences. `count` reads several inputs at once, a bounded number at a time. It is a
cobra command tree, with `golang.org/x/sync/errgroup` for the fan-out and `golang.org/x/text/cases`
for case-insensitive matching.

## Run it

```sh
go run ./apps/01-cli-wordfreq/cmd/wordfreq count --top 5 README.md CLAUDE.md
go run ./apps/01-cli-wordfreq/cmd/wordfreq count --json --top 3 --min-len 5 README.md
printf 'b\na\nb\nc\na\n' | go run ./apps/01-cli-wordfreq/cmd/wordfreq dedupe --count
go run ./apps/01-cli-wordfreq/cmd/wordfreq count --log-level info --top 1 README.md
go run ./apps/01-cli-wordfreq/cmd/wordfreq count README.md missing.txt
```

Run `count` with no operands and press Ctrl-C twice to see question 1 happen.

## Questions

Answer from the code first, then open the answer.

1. Run `count` with no operands at a terminal, so that it waits on standard input, and press
   Ctrl-C. Nothing happens; a second Ctrl-C kills the process. Why does the first one not stop it,
   and what has changed by the time the second one arrives?

   <details><summary>Answer</summary>

   `signal.NotifyContext` installs a handler for SIGINT, so the signal no longer terminates the
   process; it only cancels `ctx`. Cancelling a context closes a channel and nothing else, and the
   goroutine reading standard input is blocked inside `Read`, which no context can interrupt:
   `contextReader` looks at `ctx` only between reads. `context.AfterFunc(ctx, stop)` calls `stop` as
   soon as the first signal lands; that unregisters the handler, so the second SIGINT gets the
   default action and kills the process. `main` calls `stop()` itself instead of deferring it
   because `os.Exit` skips deferred calls. Rule: cancellation reaches only code that checks the
   context, so a program that catches signals needs another way out when that code is stuck.

   </details>

2. `slow-producer | wordfreq dedupe` shows nothing for a long time, although new distinct lines
   keep arriving and the help text says they are written while input is still being read. What
   holds them back, and why does the command call `Flush` even when scanning failed?

   <details><summary>Answer</summary>

   The lines go into a `bufio.Writer`, which keeps writes in a 4 KiB buffer and passes them on only
   when the buffer fills or `Flush` is called, and the command flushes once, at the end. A short
   line can wait until a few hundred more arrive or the input ends. When scanning fails, for example
   because the second input is missing, the command still flushes, so the lines already accepted
   reach stdout before the error, and `if flushErr := bw.Flush(); err == nil` keeps the first error
   instead of letting the flush result overwrite it. Flushing after every line would make output
   immediate at the cost of one write system call per line. Rule: a `bufio.Writer` shows nothing
   until it is flushed, so every exit path needs a `Flush` whose error is checked.

   </details>

3. `wordfreq.CountAll` and `dedupe.Set.ScanAll` each declare their own `Opener` interface and
   never import package `input`, yet both are handed an `input.Opener`. For a file, `Open` returns
   `readCloser{Reader: ..., Closer: f}`, a struct with no methods of its own. Why does all of this
   type-check, and why does standard input get `io.NopCloser` instead?

   <details><summary>Answer</summary>

   A Go type satisfies an interface by having its methods; there is no `implements` clause, so
   `input.Opener` fits both consumer-declared `Opener` interfaces and neither side imports the other.
   Embedding a field promotes its methods to the outer type: `readCloser` gets `Read` from the
   embedded `*contextReader` and `Close` from the embedded `*os.File`, so reads go through the
   context check while `Close` still closes the file. `Opener.Stdin` is a plain `io.Reader` (a
   `strings.Reader` in tests) with no `Close` to promote, and it is not the command's to close;
   `io.NopCloser` adds a no-op one so callers can close whatever `Open` returns. Rule: declare small
   interfaces where they are consumed, and build implementations from parts by embedding.

   </details>

4. `count --jobs 4` gets ten files and the third one does not exist. What happens to the files
   being read at that moment, to the ones not started yet, and what does the user see?

   <details><summary>Answer</summary>

   `errgroup.WithContext` cancels the group's context as soon as any function passed to `g.Go`
   returns an error, and `g.Wait` returns that first error only; `SetLimit(4)` makes `g.Go` block
   until a slot frees. The failed open cancels the context, but nothing is stopped by force: inputs
   in flight fail with `context.Canceled` at their next `Read`, because `contextReader` checks the
   context around every read. The loop still starts a goroutine for each remaining name, and each
   one opens its file (`os.Open` takes no context) and fails on its first read. The user sees
   `wordfreq: count: open x.txt: no such file or directory`, exit status 1, and no table. Rule: an
   errgroup cancels a context, it does not stop goroutines; work ends only where code checks it.

   </details>

5. Each goroutine in `CountAll` counts into a map of its own, with a `cases.Fold()` caser of its
   own, and takes the mutex only to merge its result. What would go wrong with one shared map that
   every goroutine increments, or with a single caser at package level?

   <details><summary>Answer</summary>

   Go maps are not safe for concurrent writes: the runtime detects them and aborts the whole process
   with `fatal error: concurrent map writes`, which `recover` cannot catch. A shared map would need
   the mutex around every increment, serializing the counting. A `cases.Caser` keeps internal state,
   and its documentation says not to share one between goroutines, so a package-level caser would be
   a data race as well as global mutable state. With private state the lock is taken once per input.
   Each closure also gets its own `name`: since Go 1.22 every loop iteration declares a fresh
   variable, whereas Python closures bind late. Rule: give each goroutine its own mutable state and
   combine the results under a lock at the end.

   </details>

6. Inputs finish in whatever order the scheduler picks, and ranging over a Go map visits keys in
   random order. Why is the output of `count` identical on every run?

   <details><summary>Answer</summary>

   Merging only adds numbers, and addition does not care about order, so the final `Counts` is the
   same whichever goroutine merges first. Map iteration order is deliberately randomized, so `Top`
   copies the entries into a slice and sorts it with a comparator that leaves no ties: `cmp.Or`
   returns its first non-zero argument, so the count decides and the word breaks ties. Without the
   word comparison, words with equal counts could swap places between runs, because
   `slices.SortFunc` is not stable and its input comes from a map. Rule: never let map iteration
   order reach output; sort with a comparator that defines a total order.

   </details>

7. In `countInput`, a file is read to the end but its `Close` fails. Does the caller find out?
   What would change if the results were unnamed, `(Counts, error)`?

   <details><summary>Answer</summary>

   `return counts, nil` first assigns the named results, then runs the deferred functions, and only
   then returns to the caller, so a deferred closure can still replace `err`. Here it sets
   `err = errors.Join(err, rc.Close())`, so a failed `Close` reaches the caller, and
   `errors.Join(nil, nil)` is nil. With unnamed results the deferred closure could only assign to a
   local variable, and the `Close` error would be lost silently. Results are either all named or all
   unnamed, hence the `_` for the counts. Rule: to report an error from a deferred call, name the
   error result and assign to it in the deferred closure.

   </details>

8. `ScanWords` checks `utf8.FullRune` before decoding and in several places returns
   `start, nil, nil`. What is it guarding against, and why do its tests replay every case through
   `iotest.OneByteReader`?

   <details><summary>Answer</summary>

   A `bufio.SplitFunc` sees whatever the `Scanner` has buffered, and a read can end mid-word or
   mid-rune: Go strings hold UTF-8 bytes, and a Cyrillic letter takes two (hence
   `utf8.RuneCountInString`, not `len`, for `--min-len`). `FullRune` catches a rune cut in half,
   which `DecodeRune` would report as `RuneError`, a separator. Returning `start, nil, nil` keeps the
   skipped separators consumed and asks for more data; only at EOF may a token end with the buffer.
   `OneByteReader` makes every byte boundary a read boundary, so all of these paths run. A token must
   fit the buffer (64 KiB by default, 16 MiB here via `Scanner.Buffer`) or the scan fails with
   `bufio.ErrTooLong`. Rule: never assume a chunk from a reader ends on a rune or token boundary.

   </details>
