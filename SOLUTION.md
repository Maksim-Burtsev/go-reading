# 002 · notes: filter GET /notes by tag, with an optional limit

4 defects, 2 decoys.

## 1. `ListByTag` read-locks twice and deadlocks under a writer (high)

`apps/02-http-notes/internal/notes/store.go:112`, in `Store.ListByTag`

What goes wrong: `ListByTag` holds `s.mu.RLock()` for the whole loop and calls `s.Get(id)` for each
note, and `Get` takes `s.mu.RLock()` again. `sync.RWMutex` blocks new readers as soon as a writer is
waiting, so the inner `RLock` queues behind a writer that is itself waiting for the outer `RLock` to
be released: neither can proceed. A `GET /notes?tag=home` that overlaps any `POST`, `PUT` or
`DELETE` deadlocks the store. From then on every request that touches it hangs until the timeout
handler answers 503 and leaks its goroutine, while `/healthz` keeps answering 200, so nothing
restarts the process (one reader and one writer in a loop deadlocked within 200 ms). The tests call
`ListByTag` sequentially and the race detector does not detect deadlocks, so CI stays green.

The tell: a method that holds `s.mu` calls another exported method of the same type, and `Get`, a
few lines above, takes `s.mu` itself.

Fix:

```go
	for id := range ids {
		n, ok := s.notes[id]
		if !ok {
			continue
		}
		out = append(out, n.clone())
	}
```

Rule: Go's locks are not reentrant, and `RWMutex` read locks are no exception; code that holds a
lock must not call anything that takes the same lock again.

Found if: the verdict names the `Get` call (a recursive `RLock`) inside `ListByTag` as a deadlock.

## 2. Empty lists turn into `"notes": null` (medium)

`apps/02-http-notes/internal/httpapi/notes.go:84`, in `listNotes`; the weakened assertion at
`apps/02-http-notes/internal/httpapi/handler_test.go:335`

What goes wrong: the response used to start from `make([]noteResponse, 0, len(all))`. Now
`var resp listNotesResponse` leaves `resp.Notes` nil when nothing is found, and `encoding/json`
encodes a nil slice as `null`. An empty store and an unknown tag both answer `{"notes":null}`
instead of `{"notes":[]}`: a breaking change for every existing client of `GET /notes`, although
`PR.md` says that without parameters the endpoint behaves as before. A Python client doing
`for n in body["notes"]` raises `TypeError`. The diff replaces the assertion that pinned `[]`,
`require.NotNil(t, resp.Notes)`, with `require.Len`, which passes for nil, and the new "unknown tag"
case compares titles copied into a slice built with `make`, so it cannot see `null` either.

The tell: the `make(..., 0, len(all))` line becomes a zero value, and the same diff deletes the
`NotNil` assertion that guarded it.

Fix:

```go
	resp := listNotesResponse{Notes: make([]noteResponse, 0, len(found))}
```

and keep `require.NotNil(t, resp.Notes)`.

Rule: a nil slice marshals as `null` and an empty one as `[]`; when a diff weakens the assertion
that guards the code it changes, read that change twice.

Found if: the verdict says an empty result is now serialized as `null`, or that the `NotNil`
assertion was dropped to let a nil slice through.

## 3. The limit is applied while ranging over a map, before sorting (medium)

`apps/02-http-notes/internal/notes/store.go:109`, in `Store.ListByTag`

What goes wrong: `ids` is a map, and Go randomizes map iteration order, so the loop keeps the first
`limit` notes it happens to visit and only then sorts them. `GET /notes?tag=work&limit=2` with ten
notes tagged `work` returns two arbitrary ones, different between identical requests, instead of
"the first N notes in list order" that `PR.md` promises (20 calls with a limit of 3 over 10 tagged
notes gave 8 different answers). The tests check only the length of a limited result
(`handler_test.go:381`, `store_test.go:147`), so they pass whichever notes come back.

The tell: a `break` on `len(out) == limit` inside a `range` over a map, with the sort after the
loop; the unfiltered branch in `listNotes` truncates after sorting.

Fix:

```go
	slices.SortFunc(out, compareNotes)
	if limit > 0 {
		out = out[:min(limit, len(out))]
	}
	return out
```

with the `break` removed from the loop.

Rule: truncate only after ordering; stopping early inside a map iteration keeps a random subset.

Found if: the verdict says a filtered list with a limit returns an arbitrary subset because the
limit is applied during map iteration, before the sort.

## 4. `Update` never removes a note from its old tags (medium)

`apps/02-http-notes/internal/notes/store.go:136`, in `Store.Update`

What goes wrong: `Update` adds the note to the index under its new tags but never removes it from
the old ones. Create a note tagged `home`, then `PUT` it with `"tags":["work"]`:
`GET /notes?tag=home` still returns it, now carrying `"tags":["work"]`, and every re-tag leaves
another stale entry. No test changes a note's tags and then filters.

The tell: `Create` indexes and `Delete` calls `unindex`, but `Update`, which replaces the whole tag
set, only indexes.

Fix:

```go
	s.notes[id] = n
	s.unindex(id)
	s.index(id, n.Tags)
```

Rule: a derived index needs every write path that changes the indexed field to remove the old
entries as well as add the new ones.

Found if: the verdict notes that updating a note's tags leaves it listed under its old tags.

## Not defects

### Deleting from `s.byTag` while ranging over it

`apps/02-http-notes/internal/notes/store.go:163`: in Python, deleting keys from a dict while
iterating over it raises `RuntimeError: dictionary changed size during iteration`. In Go it is
defined behaviour: deleting the current entry is safe, and an entry removed before the loop reaches
it is simply not produced, so `unindex` is correct.

### Reading a tag that is not in the index

`apps/02-http-notes/internal/notes/store.go:106`: `s.byTag[tag]` for an unknown tag would be a
`KeyError` in Python. In Go, indexing a map with a missing key returns the zero value, here a nil
inner map; its `len` is 0 and `range` over it does nothing, so an unknown tag yields an empty list.
Only writing to a nil map panics, and `index` creates the inner map before writing (line 155).

## Also acceptable

- `unindex` walks every tag in the index on each delete although the note's own tags are known, so
  a delete costs more as the tag vocabulary grows.
- `ListByTag` clones and sorts while holding the read lock, unlike `List`, which sorts after
  unlocking.
- `ListByTag` silently skips an index entry whose note is missing, which hides an inconsistent
  index; it also makes the delete half of `TestStoreListByTag` unable to fail, since the test would
  pass even if `Delete` never called `unindex`.
- An empty `?limit=` is treated as no limit, although `PR.md` says a limit that is not a positive
  integer is a 400.
- The app's `READING.md` still cites line numbers that the change shifts and the
  `make([]noteResponse, 0, len(all))` in `listNotes` that it removes.
