# count: add --keep-going to skip unreadable inputs

`wordfreq count docs/**/*.md` stops at the first file it cannot open or read and prints nothing,
which makes it useless on large trees with a few broken symlinks or unreadable files.

This adds `--keep-going` (`-k`). Inputs that cannot be opened or read are skipped: each one is
logged as a warning on stderr, the table covers everything that was read, and the JSON output gains
a `skipped` list. The exit status is 1 whenever an input was skipped, so scripts still notice.
Without the flag nothing changes.

`wordfreq.CountAll` gets `Options.KeepGoing` and returns the skipped inputs, sorted by name, next
to the counts.

Tests: `TestCountAll` covers a missing input and a read error in keep-going mode; `TestRun` covers
the flag end to end, for the table and for JSON.
