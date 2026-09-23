# 10 · tui-app

A two-pane terminal browser for a JSON task list: move through the tasks, filter them by title or tag,
read a task's details, and toggle it done. Toggled statuses are written back to the file when you quit.
It is built on Bubble Tea v2, which runs the Elm architecture (a model, an `Update` function and a
`View`), with Bubbles components and Lip Gloss styling.

## Run it

```sh
go run ./apps/10-tui-app/cmd/tui-app

# quitting after a toggle rewrites the bundled file; this restores it
git checkout -- apps/10-tui-app/tasks.json
```

Keys: j/k move, enter switches panes, / filters, esc clears the filter, x toggles done, q or ctrl+c
quits and saves any toggles. `-file` or `TASKS_FILE` opens another task file.

## Questions

Answer from the code first, then open the answer.

1. `run` cancels its context on SIGINT and SIGTERM, hands it to Bubble Tea with `tea.WithContext`, and
   also passes `tea.WithoutSignalHandler()`. A user quits with ctrl+c; another session is ended with
   `kill -TERM`. What happens to toggled statuses in each case, and what would change without
   `WithoutSignalHandler`?

   <details><summary>Answer</summary>

   In raw mode ctrl+c is not turned into SIGINT: the byte arrives on stdin, `Update` matches `ForceQuit`
   and returns `tea.Quit`, and `run` saves once `Run` returns cleanly. SIGTERM cancels `ctx` through
   `signal.NotifyContext`: Bubble Tea stops, restores the terminal and returns an error wrapping
   `tea.ErrProgramKilled` and `context.Canceled`, and `run` returns before `task.Save`, so the toggles
   are lost and the exit status is 1. Without `WithoutSignalHandler`, Bubble Tea would subscribe to the
   same signals (`os/signal` delivers to every registered channel) and turn SIGTERM into a `QuitMsg`, a
   normal quit that saves, racing the cancellation. Rule: let one component own process signals, and
   tell libraries that install their own handlers to stand down.

   </details>

2. While the filter input is focused, typing q adds a q to the query, yet ctrl+c still quits. Where in
   `Update` is that decided, and what would break if the `ForceQuit` check moved below the focus switch?

   <details><summary>Answer</summary>

   `Update` handles `tea.WindowSizeMsg`, then checks for `ForceQuit`, and only then routes. While
   `focus` is `focusFilter`, every message goes to `updateFilter`, which intercepts enter and esc and
   passes everything else, q included, to the `textinput` model. The `Quit` binding is matched only in
   `updateBrowse`, which filter mode never reaches. Moved below the switch, the `ForceQuit` check would
   never see a key in filter mode: ctrl+c would go to the text input and do nothing, and the user would
   have to press esc or enter first. Rule: in a message-driven UI the order of checks in `Update`
   decides which messages are global and which belong to a mode, so global ones come first.

   </details>

3. Pressing q makes `updateBrowse` return `tea.Quit`, and the file is saved in `run` after `program.Run`
   returns rather than in `Update`. Why is quitting expressed as a returned value, and why must the save
   stay out of `Update`?

   <details><summary>Answer</summary>

   `tea.Cmd` is `func() tea.Msg`, and `tea.Quit` is passed as a function value, not called. The program
   runs each returned command on its own goroutine and feeds the message it produces back into the event
   loop; a `QuitMsg` ends the loop. `Update` and `View` run on that single loop goroutine, one message
   at a time, so blocking I/O in `Update` would freeze input and rendering. `Update` would also stop
   being a function of model and message that tests can call directly, as these tests do before checking
   `cmd()` for a `QuitMsg`. Commands run concurrently and in no set order, so work that must finish
   before exit happens after `Run` returns the final model. Rule: `Update` computes the next state and
   describes side effects as commands, the same discipline as a Redux reducer.

   </details>

4. `Update` has a value receiver, so it works on a copy of the model, and pointer-receiver helpers such
   as `toggleDone` change that copy. Why does `toggleDone` still call `slices.Clone` before it writes a
   status, and why does `applyFilter` build a new `visible` slice instead of appending to
   `m.visible[:0]`?

   <details><summary>Answer</summary>

   A pointer method called on the local `m` gets `&m` automatically, because a local variable is
   addressable, so the helper's writes land in the copy `Update` returns; that covers plain fields such
   as `focus` and `cursor`. Copying the struct copies the `tasks` slice header, not its array, so the
   copy, the previous model and the slice `run` got from `task.Load` share elements, and
   `m.tasks[i].Status = ...` would change all of them. `slices.Clone` gives the new model its own array
   first, which `TestUpdateToggleDone` checks. Appending to `m.visible[:0]` would overwrite the previous
   model's indices the same way. Rule: a value receiver protects fields, not the memory their slices,
   maps and pointers refer to.

   </details>

5. Entering filter mode is written as `cmd := m.filter.Focus()` followed by `return m, cmd`, and `Focus`
   has a pointer receiver. What could go wrong with the shorter `return m, m.filter.Focus()`?

   <details><summary>Answer</summary>

   `Focus` sets the text input's focus flag through its pointer receiver, so the call changes `m`, and
   the model returned must be read after it. The spec evaluates the calls in a statement left to right,
   but it does not say when a plain operand such as `m` is read relative to them. If `m` were copied
   first, the program would get back an unfocused input, and `textinput` ignores every key until it is
   focused, so typing after `/` would do nothing. gc happens to make the call first, so the one-liner
   passes the tests today, but no rule guarantees it; the same trap hides in `return x, f(&x)`.
   Rule: when a call in a statement mutates a variable the same statement also reads, split the
   statement so the order is explicit.

   </details>

6. `Date` embeds `time.Time`. When a task file is loaded and saved, which methods does `encoding/json`
   call for the due date, and how does a task without a due date come back out without a `due` key?

   <details><summary>Answer</summary>

   Embedding promotes the methods of `time.Time` to `Date`, so `IsZero`, `Format` and `Before` work on a
   `Date` directly; the field is still named `Time`, which is how `UnmarshalJSON` assigns `d.Time`.
   `Date` declares its own `MarshalJSON` and `UnmarshalJSON`, which are shallower than the promoted
   `time.Time` ones and win, switching the wire format from RFC 3339 to `YYYY-MM-DD`. `UnmarshalJSON`
   has a pointer receiver because it writes the value; `encoding/json` calls it through the field's
   address, and `null` leaves the zero value. On save, the `omitzero` tag option calls the promoted
   `IsZero`, so a zero `Date` is dropped. Rule: embedding gives the outer type the inner type's methods,
   and a method of the same name on the outer type overrides just that one behaviour.

   </details>

7. `task.Load` reports every validation problem at once as one error. How can a caller tell a duplicate
   id from a malformed file, and what does the double `%w` in the decode path give it?

   <details><summary>Answer</summary>

   `validate` wraps each problem with `%w` around a sentinel, `ErrInvalidTask` or `ErrDuplicateID`, and
   combines them with `errors.Join`. `errors.Is` walks the whole tree, every joined error and every
   wrap, so `errors.Is(err, task.ErrDuplicateID)` finds a duplicate even when it is the third problem
   listed. Decode failures use `fmt.Errorf("%w: %w", ErrMalformed, err)`, which wraps both operands: a
   caller can test for `ErrMalformed` and still reach the cause, such as `io.ErrUnexpectedEOF` for a
   truncated file, or a `*time.ParseError` for a bad due date via `errors.As`. Rule: expose sentinels or
   error types for the conditions callers branch on, wrap with `%w`, and match with `errors.Is` and
   `errors.As`, never on message text.

   </details>

8. `task.Save` could have been one `os.WriteFile`. Instead it resolves a symlink at the path, writes a
   temporary file next to the target with the target's permission bits, syncs it, and renames it over
   the target. What does the rename buy, and why does it need the other steps?

   <details><summary>Answer</summary>

   `os.WriteFile` truncates the file and then writes it, so a crash or a full disk midway leaves a
   truncated task file. A rename within one filesystem swaps the directory entry atomically, so readers
   see the old file or the complete new one; the temporary file sits beside the target because a rename
   cannot cross filesystems. `Sync` makes the data durable before the rename is, or a power loss can
   keep the rename and lose the data. A rename replaces the entry rather than editing the file: the new
   file would get `os.CreateTemp`'s mode 0600 and a symlink would be replaced, hence `Chmod` and
   `filepath.EvalSymlinks`. Rule: write-then-rename gives atomic replacement, fsync makes it durable,
   and mode or links survive only if you carry them over.

   </details>
