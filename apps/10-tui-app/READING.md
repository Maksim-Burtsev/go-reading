# 10-tui-app

A two-pane terminal browser for a JSON task list, built on Bubble Tea v2 (Model-Update-View) and Lip Gloss.

## Run

`go run ./apps/10-tui-app/cmd/tui-app` from the repository root (`-file path/to/tasks.json` or `TASKS_FILE` picks another file; toggled statuses are written back to the file on quit).

## Where to start

1. `cmd/tui-app/main.go:run` — the whole lifecycle: flags, load, run the program, save the final model.
2. `internal/task/file.go:Load` — strict JSON decoding and validation that reports every problem at once.
3. `internal/ui/model.go:Update` — the message router: window size, force quit, then filter or browse mode.
4. `internal/ui/model.go:updateBrowse` — every key binding outside filter mode and the state change it causes.
5. `internal/ui/view.go:View` — the frame rendered from state: header, two panes, footer.

## Data flow

1. `run` loads and validates `tasks.json` into `[]task.Task` and builds the initial `ui.Model`.
2. `tea.Program` puts the terminal in raw mode and decodes stdin bytes into `tea.KeyPressMsg` values.
3. Each message goes to `Model.Update`, which returns a new model and an optional `tea.Cmd`.
4. Browse keys move the cursor, switch panes, or toggle a status; filter keys go to the `textinput` child, and `applyFilter` recomputes the visible indices.
5. Cursor, filter, and size changes re-render the selected task into the `viewport` child through `syncDetails`.
6. After every update the program calls `View`, and the renderer writes only the changed cells to stdout.
7. `q` or `ctrl+c` return `tea.Quit`; `Run` hands back the final model, and `run` saves it atomically if anything changed.

## Go specifics here

1. `internal/ui/model.go:90` — value receiver on `Update`
   <details><summary>Explanation</summary>

   `m` is a copy of the model the program holds. Every assignment inside `Update` changes only that copy, and the copy is returned as the new state. Nothing changes unless it is returned, so a `return m, nil` at the right place is the whole update. The private helpers (`resize`, `applyFilter`, `toggleDone`) have pointer receivers; calling them on the local copy `m` is legal because a local variable is addressable, and Go takes `&m` automatically.
   </details>

2. `internal/ui/model.go:228` — `slices.Clone` before a write
   <details><summary>Explanation</summary>

   Copying a struct copies the slice header (pointer, length, capacity), not the elements. Without the clone, `m.tasks[i].Status = ...` would write into the backing array shared with the previous model, so the "old" state would change too. `TestUpdateToggleDone` checks that the initial model and the caller's slice stay untouched. `applyFilter` allocates a fresh `visible` slice for the same reason instead of reusing `m.visible[:0]`.
   </details>

3. `internal/task/task.go:53` — struct with an embedded `time.Time`
   <details><summary>Explanation</summary>

   An embedded field has no name; its methods are promoted, so `t.Due.IsZero()`, `t.Due.Format(...)` and `t.Due.Before(...)` work directly on `Date`. `Date` declares its own `MarshalJSON`/`UnmarshalJSON`, which take precedence over the promoted `time.Time` ones and switch the wire format to `YYYY-MM-DD`. The `omitzero` tag option calls the promoted `IsZero` to drop a missing due date. `UnmarshalJSON` has a pointer receiver because it must modify the value.
   </details>

4. `internal/ui/model.go:114` — `return m, tea.Quit`
   <details><summary>Explanation</summary>

   `tea.Cmd` is `func() tea.Msg`. `tea.Quit` is passed as a function value, not called. The program runs returned commands on its own goroutines and feeds their results back as messages; when `tea.Quit` runs it returns a `tea.QuitMsg`, which stops the event loop. `Update` itself never performs I/O, which is why the tests call it directly and inspect the returned command with `cmd()`.
   </details>

5. `cmd/tui-app/main.go:72` — `final.(ui.Model)` with `ok`
   <details><summary>Explanation</summary>

   `Run` returns the final state as the `tea.Model` interface. A type assertion recovers the concrete `ui.Model` so `Modified` and `Tasks` are reachable. The two-value form never panics; the one-value form `final.(ui.Model)` would panic on a mismatch. It asserts `ui.Model`, not `*ui.Model`, because `Update` returns the model by value.
   </details>

## Questions

1. While the filter input is focused, why does `q` become part of the query instead of quitting, and why does `ctrl+c` still quit?
   <details><summary>Answer</summary>

   `Update` checks `ForceQuit` (`ctrl+c`) before anything else, then routes every message to `updateFilter` while `focus == focusFilter`. `updateFilter` only intercepts `enter` and `esc`; everything else, including `q`, goes to the `textinput` model. The `Quit` binding is matched only in `updateBrowse`, which is never reached in filter mode.
   </details>

2. The user moves to the third task and types a query that the task still matches. Which task is selected afterwards, and what if it no longer matches?
   <details><summary>Answer</summary>

   `applyFilter` remembers the selected task index before recomputing `visible`, then looks it up with `slices.Index`. If it is still visible, the cursor moves to its new position, so the selection survives. If not, `slices.Index` returns -1 and `max(-1, 0)` puts the cursor on the first match.
   </details>

3. How can a caller of `task.Load` tell a duplicate ID apart from a malformed file, given that validation returns all problems as one error?
   <details><summary>Answer</summary>

   `validate` wraps each problem with `%w` around a sentinel (`ErrInvalidTask`, `ErrDuplicateID`) and combines them with `errors.Join`. `errors.Is(err, task.ErrDuplicateID)` walks the joined tree and finds it. Decoding failures are wrapped as `ErrMalformed` with a double `%w`, so the underlying `json` error stays reachable too.
   </details>

4. If the process receives SIGTERM while the TUI is open, are toggled statuses saved? What does the process exit with?
   <details><summary>Answer</summary>

   No. `signal.NotifyContext` cancels `ctx`; `tea.WithContext(ctx)` makes the program stop, restore the terminal, and return an error wrapping `tea.ErrProgramKilled` and `context.Canceled`. `run` returns that error before reaching `task.Save`, and `main` exits with status 1. Only `tea.Quit` (`q`, `ctrl+c`) ends `Run` without an error and leads to the save.
   </details>

5. Why does `task.Save` write to a temporary file and rename it instead of writing to the path directly?
   <details><summary>Answer</summary>

   A crash or a full disk in the middle of a direct write would leave a truncated task file. `os.CreateTemp` in the same directory followed by `os.Rename` replaces the file in one step on the same filesystem, so readers see either the old or the new content. The deferred `os.Remove` cleans up the temporary file when any step before the rename fails; after a successful rename it has nothing to remove.
   </details>
