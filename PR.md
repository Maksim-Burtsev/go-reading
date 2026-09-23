# Autosave toggled statuses

Toggled statuses were only written on a clean quit, so a SIGTERM, a closed terminal or a crash threw
them away. Each toggle now saves the file in the background through a `tea.Cmd`, and the header
shows `saving…` until the save reports back, or the error when it fails. Quitting while a save is in
flight waits for it to report back, so the process never exits halfway through a write; the save on
quit stays as a fallback. Toggling also no longer copies the whole task list on every keypress,
since the list is saved right away.

`ui.New` takes the save function, so the model tests pass a stub. `TestToggleSavesInBackground` runs
the save command and checks what was saved, that the model is clean afterwards and that the header
clears; `TestQuitWaitsForSave` checks that q during a save quits only once the save reports back;
`TestFailedSaveKeepsChangesUnsaved` checks that a failed save is shown and leaves the changes
unsaved. `TestRun` now checks the saved file instead of the "tasks saved" log line, since the
autosave writes the file before the program exits.
