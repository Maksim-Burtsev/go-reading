# 010 · Autosave toggled statuses

5 defects, 2 decoys.

## 1. The model writes into a slice it shares with an in-flight save (high)

`apps/10-tui-app/internal/ui/model.go:264`, in `toggleDone`

What goes wrong: `toggleDone` no longer clones before writing. It sets `Status` in place in
`m.tasks`, whose backing array is shared by every earlier model value and by the slice `run` got from
`task.Load`. At line 272 the same slice goes to the save command, which encodes it on another
goroutine. Press x on one task and x on another before the first save finishes. The second `Update`
writes into the array while the first save's `json.MarshalIndent` reads it: a data race on a string
header, so the file can capture a mix of both states. `-race` reports it as soon as a real program
runs a slow save. `Update` has a value receiver precisely so each state is a snapshot, and earlier
models, such as the tests' `initial`, now see later toggles.

The tell: the deleted `m.tasks = slices.Clone(m.tasks)`, which PR.md presents as saving a copy per
keypress, together with the two assertions deleted from `TestUpdateToggleDone`. Those assertions
checked exactly this: that the caller's slice and the initial model stay untouched.

Fix:

```go
	m.tasks = slices.Clone(m.tasks)
	if m.tasks[i].Status == task.StatusDone {
```

and restore `require.Equal(t, sampleTasks(), input)` and `require.Equal(t, sampleTasks(), initial.Tasks())`.

Rule: never write to a slice after handing it to another goroutine. Copy-on-write (clone, then
modify) gives each state an immutable snapshot that is safe to share.

Found if: the verdict names the removed `slices.Clone` in `toggleDone`, or the save command sharing
`m.tasks`, and says later toggles write into the array that a running save reads, or that earlier
models change.

## 2. A failed save clears the dirty flag (high)

`apps/10-tui-app/internal/ui/model.go:128`, in `updateSaved`

What goes wrong: a save fails, because the directory is read-only, the disk is full or the path is now
a directory. `updateSaved` sets `modified` to false anyway. The header shows the error, but the model
reports no unsaved changes, so on quit `run` sees `!model.Modified()` and skips its fallback save. The
toggles are lost and the process exits 0. The fallback that PR.md keeps "as a fallback" is dead exactly
when it is needed.

The tell: `m.modified = false` sits next to `m.saveErr = saved.err`, and nothing checks the error first.

Fix:

```go
	m.saving = false
	m.saveErr = saved.err
	if saved.err == nil {
		m.modified = false
	}
```

Rule: clear a dirty flag only when the write it tracks has succeeded. An error path that resets the
same state as the success path loses data silently.

Found if: the verdict names `m.modified = false` running on failure too, so a failed autosave also
cancels the save on quit.

## 3. The save result is routed through the mode switch (medium)

`apps/10-tui-app/internal/ui/model.go:118`, in `Update`

What goes wrong: `savedMsg` is handled only in the `default` branch of the routing switch. While the
filter input is focused, the first case (line 113) sends every message to `updateFilter`, which hands
it to the text input, which ignores it. Press x and then `/` before the save reports back, and the
`savedMsg` disappears. `saving` stays true: the header shows `saving…` for good, and a failure is never
shown. If the user then presses ctrl+c, `quit` waits for a save report that never comes, and every
further ctrl+c waits again. The program cannot be quit until it is killed.

The tell: handling for a new message type sits in the `default` of a switch whose first case captures
every message in filter mode. `tea.WindowSizeMsg`, which has to work in every mode, is handled above
the switch for exactly that reason.

Fix:

```go
	if saved, ok := msg.(savedMsg); ok {
		return m.updateSaved(saved)
	}
	keyMsg, isKey := msg.(tea.KeyPressMsg)
```

with `updateSaved` taking a `savedMsg`, and `default: return m, nil` restored.

Rule: in a message-driven UI, dispatch messages that must be handled in every mode, such as command
results and resizes, before routing by mode.

Found if: the verdict says `savedMsg` is handled only in the `default` branch, so it is lost while the
filter is focused (stuck `saving…`, lost error, or a quit that never happens).

## 4. Saves are not ordered, and one bool tracks several of them (medium)

`apps/10-tui-app/internal/ui/model.go:272`, in `toggleDone`

What goes wrong: every toggle returns its own save command, and Bubble Tea runs each command on its own
goroutine with nothing ordering them. With defect 1 fixed, each save gets its own snapshot: toggle a
task, so save A snapshots `done`, then toggle it back, so save B snapshots `todo`. If A is slower, for example stuck in fsync, B renames first and A renames
last, so the file says `done` while the screen says `todo`, and no error appears. Quitting has the same
hole: `saving` is one bool, so with two saves in flight the first report clears it and releases a
pending quit (line 138) while the other save is still writing. The process then exits, leaving the
older snapshot on disk and a stray temp file, despite PR.md's promise that it "never exits halfway
through a write".

The tell: a write that has to be ordered, the same file on every keypress, issued as a fire-and-forget
command per keypress, plus a boolean `saving` standing in for a count.

Fix:

```go
	m.modified = true
	m.syncDetails()
	if m.saving {
		m.dirty = true
		return nil
	}
	m.saving = true
	return saveTasks(m.save, m.tasks)
```

with a `dirty bool` field. In `updateSaved`, after a report: if `m.dirty`, clear it and start one more
save of the current tasks, and quit only once no save is in flight.

Rule: goroutines, commands or callbacks that write the same resource need an explicit order.
Fire-and-forget writers finish in any order, and a bool cannot count them.

Found if: the verdict says the saves are independent and unordered (an older snapshot can land last),
or that one `saving` bool releases the quit while another save is still in flight.

## 5. The failure test asserts before the failure (low)

`apps/10-tui-app/internal/ui/model_test.go:475`, in `TestFailedSaveKeepsChangesUnsaved`

What goes wrong: the test's name promises that a failed save leaves the changes unsaved, but its only
`Modified()` assertion runs before the failed `savedMsg` is delivered at line 477. Afterwards it checks
only the header text. It passes with defect 2 in place.

The tell: the assertion that matches the test's name comes before the event the name is about.

Fix:

```go
	m, _ = send(m, cmd())
	require.Contains(t, ansi.Strip(m.View().Content), "save failed: disk full")
	require.True(t, m.Modified())
```

Rule: assert the state after the event under test. An assertion placed before it only re-checks the
setup.

Found if: the verdict says the test checks `Modified()` before delivering the failure, so it cannot
catch the flag being cleared.

## Not defects

### A value receiver that returns its modified copy

`apps/10-tui-app/internal/ui/model.go:122`: `updateSaved`, like `quit` below it, assigns to fields of
`m` on a value receiver, which reads like writes to a throwaway copy. They return the modified copy,
`Update` returns it, and the program keeps it as the new state. `Update` itself works the same way.

### A nil command

`apps/10-tui-app/internal/ui/model.go:262`: `toggleDone` returns nil when nothing is selected, and
`updateBrowse` returns it as the command. To a Python reader, returning None where a callable is
expected looks like a crash waiting to happen. A nil `tea.Cmd` means "no side effect": Bubble Tea
skips nil commands, and the tests use `cmd != nil` to tell a toggle from a no-op.

## Also acceptable

- ctrl+c no longer forces anything: if a save hangs, for example on a stalled network filesystem,
  neither q nor ctrl+c can quit, because both wait for the save to report back.
- `TestRun` dropped its "tasks saved" assertion, so nothing tests the quit-time fallback save any more.
- `ui.New` with a nil `SaveFunc` compiles, and the first toggle then panics inside the save command.
- `SaveFunc` does file I/O but takes no `context.Context`, against the repository's rule that I/O
  functions take a context first.
