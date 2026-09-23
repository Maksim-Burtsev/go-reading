# 006 · store: support Idempotency-Key on order creation

5 defects, 2 decoys.

## 1. The race branch queries a transaction Postgres has already aborted (medium)

`apps/06-pg-service/internal/store/store.go:176`, in `CreateOrder`

What goes wrong: this is the branch for two requests with the same key in flight at once, the
usual case of a client that times out and retries while its first attempt is still running. Both
look the key up and miss, because neither has committed, and both insert an order. The second
`InsertIdempotencyKey` waits on the primary key until the first transaction commits, then fails
with 23505. In Postgres an error inside a transaction aborts it: every later statement fails with
25P02 until the transaction ends. So the lookup at line 176 fails with "current transaction is
aborted", `BeginFunc` rolls back, and the client gets 500 instead of the winner's order. The branch
cannot succeed. Even without the lookup, returning nil would make `BeginFunc` commit an aborted
transaction, which Postgres turns into a rollback. Reproduced by holding a lock on
`order_idempotency_keys` until two calls with the same key reach the insert: one returned the
order, the other `select idempotency key: ERROR: current transaction is aborted, commands ignored
until end of transaction block (SQLSTATE 25P02)`.

The tell: a query on the transaction right after a `*pgconn.PgError` from that same transaction
was caught and treated as recoverable. psycopg users know the outcome as `InFailedSqlTransaction`.

Fix: let the insert report the conflict without failing, roll back this order, and read the
winner's order after the transaction:

```sql
-- name: InsertIdempotencyKey :execrows
INSERT INTO order_idempotency_keys (user_id, key, request_hash, order_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, key) DO NOTHING;
```

```go
		n, err := q.InsertIdempotencyKey(ctx, params)
		if err != nil {
			return fmt.Errorf("insert idempotency key: %w", err)
		}
		if n == 0 {
			return errKeyTaken
		}
	// ...
	})
	if errors.Is(err, errKeyTaken) {
		prev, err := s.queries.GetOrderByIdempotencyKey(ctx, key)
		if err != nil {
			return Order{}, fmt.Errorf("select idempotency key: %w", err)
		}
		return replayOrder(prev, hash, items)
	}
```

Rule: after any error inside a Postgres transaction the only thing left to do with it is roll it
back; expected conflicts are handled with `ON CONFLICT` or a savepoint, not by querying on.

Found if: says the lookup after the unique violation runs in an aborted transaction (fails with
25P02), so racing requests with the same key get an error instead of the first order.

## 2. The key lookup ignores the user (high)

`apps/06-pg-service/internal/db/queries/orders.sql:39`, used by `CreateOrder` at `store.go:126`
and `store.go:176`

What goes wrong: the table's primary key is `(user_id, key)` (migration line 8), but the lookup
filters on `key` alone, and the generated `GetOrderByIdempotencyKey(ctx, key)` takes no user.
Clients choose the keys, and they are often guessable: a counter, a cart ID, a timestamp. When user
B posts to `/users/B/orders` with a key user A already used and the same items, the lookup finds
A's row, the hashes match, and B gets 201 with A's order (its ID, `user_id` and total) while B's
order is never placed. It happens before `LockUser`, so it works even for a user ID that does not
exist. With other items, B is refused for a key it never used. Reproduced: Bob's order with Ann's
key returned Ann's order.

The tell: the constraint is `(user_id, key)` and `userID` is in scope in `CreateOrder`, yet the
query's `WHERE` names only `k.key` and the regenerated function has a single parameter.

Fix:

```sql
-- name: GetOrderByIdempotencyKey :one
SELECT o.id, o.user_id, o.total_cents, o.created_at, k.request_hash
FROM order_idempotency_keys k
JOIN orders o ON o.id = k.order_id
WHERE k.user_id = $1 AND k.key = $2;
```

Regenerate, then call it with
`sqlc.GetOrderByIdempotencyKeyParams{UserID: userID, Key: key}`. The lookup then also uses the
primary key's index, which it could not do with the second column alone.

Rule: scope a lookup by a client-supplied key exactly as the constraint that makes the key unique,
or one tenant reads another's data.

Found if: says the lookup is not filtered by user, so a key used by another user returns that
user's order (or keys collide across users).

## 3. The request hash depends on map iteration order (medium)

`apps/06-pg-service/internal/store/store.go:274`, in `requestHash`

What goes wrong: Go randomizes the iteration order of a map, on purpose, for every `range`.
`requestHash` writes the lines in whatever order the map yields them, so an order with two
different lines hashes to one of two values at random. A client that retries a two-line order with
the same key and the same body gets `ErrIdempotencyKeyReused` whenever the order came out
differently, which with defect 4 is a 500. Measured on this code: 200 calls for one two-line order
produced two hashes (168 and 32 times), and 4 of 20 identical retries were rejected. More lines
give more permutations and more rejections. Single-item orders, the only kind the tests retry, are
unaffected.

The tell: `range` over a map feeding a hash, the one place where the output must be canonical.
Python dicts keep insertion order; Go maps promise no order at all.

Fix:

```go
	keys := slices.SortedFunc(maps.Keys(lines), func(a, b line) int {
		return cmp.Or(strings.Compare(a.sku, b.sku), cmp.Compare(a.price, b.price))
	})
	var b strings.Builder
	for _, l := range keys {
		fmt.Fprintf(&b, "%q %d %d\n", l.sku, l.price, lines[l])
	}
```

Rule: never let map iteration order reach output that has to be stable (hashes, signatures, cache
keys, serialized text); sort the keys first.

Found if: names the map iteration in `requestHash` and says the hash is not deterministic, so
identical retries can be rejected.

## 4. Reusing a key with other items is a 500, not a 422 (medium)

`apps/06-pg-service/internal/httpapi/handler.go:189`, in `fail` (the error is declared at
`store.go:41`)

What goes wrong: the PR promises 422 for a key reused with other items, but `fail` has no case for
`store.ErrIdempotencyKeyReused`, so it falls through to `default`: an ERROR log line and 500
"internal error". A 5xx tells the client to retry, and a retry with the same key fails the same way
again. With defect 3, honest retries of multi-item orders end up here too.

The tell: a new exported sentinel with a doc comment in the store, a description that promises 422,
and neither a new `case` in `fail` nor a handler test for it.

Fix:

```go
	case errors.Is(err, store.ErrIdempotencyKeyReused):
		h.writeError(w, r, http.StatusUnprocessableEntity, "idempotency key reused with other items")
```

Rule: when the storage layer gains a sentinel error, the mapping to a status code gains a case, and
a test pins the status.

Found if: says the new error is not mapped in `fail`, so key reuse returns 500 instead of 422.

## 5. The "concurrent" test runs its calls one after the other (low)

`apps/06-pg-service/internal/store/store_integration_test.go:234`, in
`testIdempotentCreateOrder`

What goes wrong: "concurrent retries with the same key place one order" makes two calls in
sequence. The second always finds the committed key in the first lookup, so the conflict branch
(defect 1) never runs. The test is the plain retry case again under another name, and it passes
while suggesting the race is handled.

The tell: "concurrent" in the name, and no goroutines or barrier in the body.

Fix: start both calls together and hold them until both are past the lookup, for example by locking
the key table in another transaction:

```go
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "LOCK TABLE order_idempotency_keys IN SHARE ROW EXCLUSIVE MODE")
	require.NoError(t, err)

	var wg sync.WaitGroup
	orders, errs := make([]store.Order, 2), make([]error, 2)
	for i := range 2 {
		wg.Go(func() { orders[i], errs[i] = s.CreateOrder(ctx, user.ID, "retry-2", items) })
	}
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting)
		return err == nil && waiting == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, orders[0].ID, orders[1].ID)
```

Rule: a test named after a race has to force the interleaving, or it tests the sequential path
twice.

Found if: says the "concurrent" test is sequential and never reaches the conflict branch.

## Not defects

### Adding to a map entry that does not exist yet

`apps/06-pg-service/internal/store/store.go:270`: `lines[line{...}] += int64(it.Quantity)` reads a
key that may not be in the map yet. In Python that raises `KeyError`; in Go, indexing a map with a
missing key yields the element type's zero value, so the first `+=` starts from 0 and no
`defaultdict` or existence check is needed. A struct whose fields are comparable is a valid map key.

### Assigning the result from inside the transaction closure

`apps/06-pg-service/internal/store/store.go:129` (and `:180`): `order, err = replayOrder(...)`
inside the `BeginFunc` closure writes the `order` declared before `BeginFunc`. In Python the
assignment would create a local unless declared `nonlocal`; a Go closure captures the variable
itself, so `CreateOrder` returns the replayed order. The `err` on that line is the one declared in
the block, so an `ErrIdempotencyKeyReused` from `replayOrder` still reaches `BeginFunc`, which rolls
back a transaction that only read.

## Also acceptable

- Keys never expire: nothing deletes old rows, so the table grows forever and a client can never
  reuse a key.
- Lines with the same SKU and price are merged before hashing, so a retry that splits or joins lines
  counts as the same request and gets back items that differ from the ones stored.
- The handler checks only the key's length. net/http passes header bytes 0x80-0xFF through,
  Postgres rejects them in a `text` column with SQLSTATE 22021, and that becomes a 500.
