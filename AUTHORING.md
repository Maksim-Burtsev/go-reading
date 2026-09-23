# Authoring

How the reading material and the review branches are written. The reader is a senior Python
backend engineer learning to read and review Go. What they take away must transfer to other Go
codebases: mechanisms and patterns, not facts about this repository.

## READING.md

One per app, `apps/NN-name/READING.md`:

````markdown
# NN · name

Two or three sentences on what the program does, as its user sees it.

## Run it

```sh
commands, run from the repository root
```

## Questions

Answer from the code first, then open the answer.

1. Question.

   <details><summary>Answer</summary>

   Answer.

   </details>
````

- Six to eight questions, ordered from the entry point inward: answering them in order is the tour
  of the app.
- Each question turns on a Go mechanism or a production pattern the reader will meet again, asked
  as a scenario: what happens when X, why A and not the obvious B, what breaks without Y.
- The reader navigates by symbol search. Name a function or type only when it is the subject of the
  question; paths and line numbers stay out of the file.
- An answer is three to eight lines: the mechanism, its consequence in this app, and a last
  sentence that starts with `Rule:` and states what to carry to other codebases. Every answer is
  checked against the code as it is.
- Run commands are ones you have seen work.

## Review branches

A review is one PR-sized change to one app with defects planted in it. The reader reviews its diff
against master and gives a verdict; the answer key lives on a separate branch.

### `review/NNN-slug`

- Cut from master. NNN is the next free number; the first review of app NN is `0NN`.
- One commit whose subject is the PR title.
- `PR.md` at the repository root: `# <PR title>`, then the description its author would write —
  what the change does and why, which tests it adds. It is the first file of the diff.
- The change is one a coding agent would plausibly produce when asked: a feature, a refactor, a
  robustness fix. It goes through the app's core mechanics (its concurrency, lifecycle or data
  path), so the defects sit in code that matters. 100 to 250 changed lines over 2 to 5 files, tests
  included.
- Everything but the planted defects is correct and written in the repository's style. A defect
  reads as an honest mistake: its names, comments and tests look as confident as the rest.
- CI stays green: `make lint test`, plus the integration tests when the change reaches them. A
  defect is what only a reader catches, so nothing the configured linters flag qualifies: unchecked
  errors, `%v` on an error in `fmt.Errorf`, `err == ErrX`, a request without a context, an unclosed
  body or rows, a copied lock, `context.Background()` with a context in scope.
- Each defect is visible in the diff without running anything, and unambiguous once pointed out.
  Mix the kinds: Go semantics, concurrency and lifecycle, error handling, API semantics, delivery
  semantics, boundaries, a test that does not test what it claims.
- One or two decoys: code that looks wrong to a Python developer and is correct Go.
- Difficulty by app: 01 has 3 defects; 02–04 have 4, at least one about concurrency; 05–08 have 4
  or 5; 09–12 have 5, one of them about design or delivery semantics.

### `review/NNN-slug-solution`

The review branch plus one commit that adds `SOLUTION.md` at the root. Line numbers refer to the
review branch.

````markdown
# NNN · PR title

N defects, M decoys.

## 1. Short name of the defect (high | medium | low)

`path/to/file.go:LINE`, in `Function`

What goes wrong: the concrete scenario, from input or state to the wrong outcome.

The tell: what in the diff should have stopped the reader.

Fix:

```go
the minimal fix
```

Rule: one sentence to carry to other codebases.

Found if: what a verdict has to name for this defect to count — the place and the gist.

## Not defects

### Decoy name

`path/to/file.go:LINE`: why it looks wrong to a Python developer, and why it is correct.

## Also acceptable

Real problems in the change that were not planted, one line each. A verdict that names one is not
a false positive.
````

### Before pushing

A fresh agent reviews the review branch blind, without the solution. Every real problem it finds is
either a planted defect, listed under "Also acceptable", or fixed on the branch; a planted defect
that cannot be justified from the diff is rewritten. Push both branches only after that.
