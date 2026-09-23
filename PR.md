# notes: filter GET /notes by tag, with an optional limit

Clients that show one tag at a time download every note and filter locally.

`GET /notes?tag=home` now returns only the notes that carry the tag. The store keeps a tag index
up to date on create, update and delete, so a filtered list no longer scans every note.
`?limit=N` returns at most the first N notes in list order, with or without a tag; a limit that is
not a positive integer is a 400 with code `invalid_query`. Without parameters the endpoint behaves
as before.

Tests: `TestStoreListByTag` covers filtering, an unknown tag, a limit and index cleanup on delete;
`TestListNotesQuery` covers the query parameters end to end.
