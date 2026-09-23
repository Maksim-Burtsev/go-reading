# 001 · count: add --keep-going to skip unreadable inputs

3 defects, 2 decoys.

## 1. Skipped inputs are appended without the lock (high)

`apps/01-cli-wordfreq/internal/wordfreq/count.go:131`, in `CountAll`

What goes wrong: in keep-going mode every goroutine whose input fails appends to the shared
`skipped` slice, and none of them holds `mu`. When two inputs fail at about the same time, the
normal case for `wordfreq count -k -j 8 docs/*.md` with a few unreadable files, the appends race on
the slice header: entries overwrite each other and skipped inputs silently vanish from the warnings
and from the JSON `skipped` list (six missing files with `-j 7` came back as four). It is a data
race, so `go test -race` reports it as soon as a test has two failing inputs; the PR's tests have
exactly one each. The `slices.SortFunc` after `g.Wait()` needs no lock: `Wait` orders every
goroutine's writes before it returns.

The tell: a variable shared by all goroutines is written three lines above the `mu.Lock()` that
guards the other shared variable, `total`; the new early-return branch never reaches the lock.

Fix:

```go
			if err != nil {
				if !opts.KeepGoing {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				skipped = append(skipped, Skipped{Name: name, Err: err})
				return nil
			}
```

Rule: every write to state shared between goroutines goes under the lock that guards it, including
the writes on early-return paths.

Found if: the verdict names the unlocked `append` to `skipped` inside the goroutine, as a data race
or as the cause of lost skipped inputs.

## 2. Cancellation is reported as skipped inputs (medium)

`apps/01-cli-wordfreq/internal/wordfreq/count.go:128`, in `CountAll`

What goes wrong: in keep-going mode every error from `countInput` becomes a skip, including the
context errors that reads return once `ctx` is cancelled. Ctrl-C, or `timeout 60s wordfreq count
-k ...`, in the middle of a run: every input being read fails with `context canceled`, every
queued input fails on its first read, each one is logged as "input skipped", and the command prints
the counts of whatever had finished as if the run had completed. With defect 3 it also exits 0.

The tell: the new branch handles every error the same way. Nothing tells "this input is
unreadable" apart from "the whole run was cancelled", although `Opener`'s doc says reads fail once
ctx is done.

Fix:

```go
				if !opts.KeepGoing || ctx.Err() != nil {
					return err
				}
```

Rule: a context error means the caller gave up, not that one item failed; never fold it into
per-item error handling.

Found if: the verdict says that cancellation (Ctrl-C, SIGTERM, a timeout) is swallowed in
keep-going mode or reported as skipped inputs.

## 3. The exit status stays 0 when inputs are skipped (high)

`apps/01-cli-wordfreq/internal/cli/count.go:104`, in `countCommand.run`; the tests at
`apps/01-cli-wordfreq/cmd/wordfreq/main_test.go:98` and `:108`

What goes wrong: `PR.md` promises exit status 1 whenever an input was skipped, "so scripts still
notice", but `run` logs the skipped inputs and returns whatever `writeJSON` or `writeTable` returns,
which is nil. `wordfreq count -k a.txt missing.txt; echo $?` prints 0, so a `set -e` script or a CI
job takes a partial count for a complete one; when every input is skipped it prints an empty table
and still succeeds. The two new `TestRun` cases have no `wantErr`, so the harness asserts
`require.NoError`: the tests pin exactly the behaviour the description rules out.

The tell: the description promises a non-zero exit, no line in the diff returns an error for
skipped inputs, and both new CLI tests expect success.

Fix:

```go
	write := writeTable
	if c.json {
		write = writeJSON
	}
	if err := write(cmd.OutOrStdout(), rep); err != nil {
		return err
	}
	if len(skipped) > 0 {
		return fmt.Errorf("count: %d of %d inputs skipped", len(skipped), len(names))
	}
	return nil
```

plus a test that checks the output and the error of the same run.

Rule: when a command reports partial success, its exit status has to say so, and its tests have to
check the status, not only the output.

Found if: the verdict notes that `run` returns nil (exit 0) although inputs were skipped,
contradicting `PR.md`, or that the new tests assert success for such a run.

## Not defects

### `skipped` starts as a nil slice

`apps/01-cli-wordfreq/internal/wordfreq/count.go:123`: `var skipped []Skipped` is never
initialized; the Python equivalent, `None`, would fail on the first `append`. In Go a nil slice is a
valid empty slice: `append` allocates on first use, `len` is 0, `range` over it in `run` does
nothing, and returning it for "nothing skipped" is idiomatic; `omitempty` then drops the JSON key.

### The goroutine closure assigns `skipped` and reads `name`

`apps/01-cli-wordfreq/internal/wordfreq/count.go:131`: to a Python reader this closure has two
problems. Assigning to `skipped` inside a nested function would create a local without `nonlocal`,
and `name` would be late-bound, so every goroutine would see the last name. Neither applies in Go:
a closure captures the variable itself, so the assignment updates the outer `skipped`, and since Go
1.22 each iteration of a `for ... range` loop has its own `name`. The same line is still defect 1,
for the missing lock.

## Also acceptable

- Skipped inputs are reported only as warn-level log records, so `--log-level error` (or
  `WORDFREQ_LOG_LEVEL=error`) hides them entirely in table mode.
- The JSON `skipped` list carries only the names; why each input was skipped exists only in the log.
- No `TestCountAll` case has two skipped inputs, so the promised name order and the `Err` field of
  `Skipped` are never checked; such a case would also have exposed defect 1 under `-race`.
