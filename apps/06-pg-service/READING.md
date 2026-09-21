# 06-pg-service

A users and orders JSON API on chi, pgx/v5, sqlc and goose, with a transactional order write and keyset pagination.

## Run

```sh
docker compose -f apps/06-pg-service/docker-compose.yml up -d && go run ./apps/06-pg-service/cmd/pg-service   # integration tests: go test -race -tags integration ./apps/06-pg-service/...
```

## Where to start

1. `cmd/pg-service/main.go:run` — the whole lifecycle: env config, pool, migrations, HTTP server, graceful shutdown.
2. `internal/db/db.go:Migrate` — how embedded SQL files become the schema on startup, and why several replicas can do it at once.
3. `internal/httpapi/handler.go:NewHandler` — the middleware stack, the routes, and the `Store` interface the handlers depend on.
4. `internal/store/store.go:CreateOrder` — the one multi-statement transaction; read it next to `internal/db/queries/orders.sql`.
5. `internal/store/store.go:ListOrders` — keyset pagination; `internal/store/cursor.go` holds the opaque cursor format.

## Data flow

1. chi matches the route; the request passes `RequestID`, the client-IP middleware, `logRequests` (which wraps the `ResponseWriter` to capture the status) and `Recoverer`.
2. The handler reads `{id}` with `r.PathValue`, decodes the body through `http.MaxBytesReader` with unknown fields rejected, and validates it.
3. It calls the handler-side `Store` interface; in production that is `*store.Store`, in tests a fake.
4. `store.Store` runs sqlc-generated methods from `internal/db/sqlc`, which were generated from `queries/*.sql` against the goose migrations.
5. `CreateOrder` runs inside `pgx.BeginFunc`: lock the user row `FOR SHARE`, insert the order, `COPY` the items, then let Postgres compute `total_cents`.
6. Postgres errors become sentinels: `pgx.ErrNoRows` → `ErrUserNotFound`, 23505 on `users_email_key` → `ErrEmailTaken`, 23514/22003 → `ErrInvalidOrder`.
7. `handler.fail` maps the sentinels to 404/409/422; anything else is logged with the request ID and returned as 500.
8. Listing fetches `limit+1` rows ordered by `(created_at, id) DESC`; the extra row turns into `next_cursor`, which the next call feeds back into `ListOrdersAfter`.

## Go specifics here

1. `internal/db/db.go:39` — assignment to `err` inside a deferred closure.
   <details><summary>Explanation</summary>

   `Migrate` has named results, so `err` is a real variable that lives until the function returns. A deferred function runs after the `return` statement has set the results, and it can still change them. Here it joins the close error of the temporary `*sql.DB` with whatever error was being returned, so a failed `Close` is not silently dropped and a real failure is not masked by it. Without named results the defer could not touch the returned value at all.
   </details>

2. `internal/store/store.go:89` — `User(row)`.
   <details><summary>Explanation</summary>

   `row` is a `sqlc.User` from the generated package and `User` is the store's own type. Go allows converting between two struct types whose fields have identical names, types and order (tags are ignored), so one conversion replaces a hand-written field-by-field mapping. It is also a compile-time contract: if a migration adds a column and sqlc regenerates `sqlc.User`, this line stops compiling until the domain type is updated. There is no duck typing here; the conversion must be explicit.
   </details>

3. `internal/store/store.go:84` — `errors.As(err, &pgErr)`.
   <details><summary>Explanation</summary>

   `pgErr` is declared as `*pgconn.PgError` and a pointer to it is passed, so `errors.As` receives a `**pgconn.PgError`. It walks the chain built by `%w` wrapping and, when it finds an error whose dynamic type is `*pgconn.PgError`, stores it through that pointer. This is the Go counterpart of `except UniqueViolation as e:`, except that the match is by type anywhere in the wrap chain, and the SQLSTATE and constraint name are then inspected as plain fields.
   </details>

4. `internal/store/store.go:112` — `pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {`.
   <details><summary>Explanation</summary>

   The transaction body is a closure. `BeginFunc` begins, calls it, commits if it returns `nil` and rolls back if it returns an error (or panics), so there is no `defer tx.Rollback` in sight. The closure cannot return the order directly because its signature is fixed to `error`; it writes to the `order` variable declared outside it (line 145) instead. Go closures capture variables by reference, which is what makes that assignment visible after `BeginFunc` returns.
   </details>

5. `cmd/pg-service/main.go:81` — `make(chan error, 1)`.
   <details><summary>Explanation</summary>

   `srv.Serve` blocks, so it runs in its own goroutine and reports how it ended through a channel. The channel has a buffer of one so the goroutine's single send never blocks: whether `run` is waiting in the `select` (line 88), reading it after `Shutdown` (line 100), or has returned because `Shutdown` failed, the goroutine can always finish and does not leak. With an unbuffered channel and no receiver the send would block forever.
   </details>

## Questions

1. Why does `ListOrders` ask the database for `limit + 1` rows?
   <details><summary>Answer</summary>

   The extra row answers "is there another page?" without a `COUNT(*)` or a second query. If it comes back, it is dropped, and the cursor is built from the last row that is returned to the client; if it does not, the page is the last one and `Next` stays `nil`, so `next_cursor` is omitted from the JSON.
   </details>

2. The second item of an order has `quantity: 0` and reaches the store. What exactly happens to the order row inserted a moment earlier?
   <details><summary>Answer</summary>

   The `COPY` into `order_items` fails with SQLSTATE 23514 from the `quantity > 0` check. The closure returns the wrapped error, `pgx.BeginFunc` rolls the transaction back, so the `orders` row and any copied items disappear. `CreateOrder` recognises the check violation with `errors.As` and returns it wrapped with `ErrInvalidOrder`, which the handler turns into 422. The integration test asserts that only the successful order's rows remain for that user.
   </details>

3. Why is a duplicate email detected by catching 23505 after the `INSERT` instead of running `SELECT ... WHERE email = $1` first?
   <details><summary>Answer</summary>

   A check-then-insert is racy: two concurrent requests can both see no row and both insert. The unique constraint is the only atomic check, so the store lets Postgres enforce it and translates the violation. Matching on `ConstraintName` as well as the code keeps a future unique constraint on another column from being reported as "email already taken".
   </details>

4. Why does pagination order and compare on `(created_at, id)` rather than on `created_at` alone, and why is the cursor timestamp stored as Unix microseconds?
   <details><summary>Answer</summary>

   `created_at` is not unique: rows written in the same transaction or the same microsecond share it, and a cursor on the timestamp alone would skip or repeat them at a page boundary. `id` breaks the tie, the row comparison `(created_at, id) < ($2, $3)` expresses "strictly after" in one predicate, and the index `(user_id, created_at DESC, id DESC)` serves it. `timestamptz` has microsecond resolution, so microseconds round-trip exactly; anything coarser would make the comparison wrong.
   </details>

5. Why does shutdown derive its deadline from `context.WithoutCancel(ctx)` instead of from `ctx`?
   <details><summary>Answer</summary>

   Shutdown starts because `ctx` was cancelled by SIGINT/SIGTERM. A timeout derived from an already-cancelled context is already done, so `srv.Shutdown` would return immediately without waiting for in-flight requests. `WithoutCancel` keeps the context's values but detaches it from the parent's cancellation, so the fresh `ShutdownTimeout` actually governs how long requests get to drain.
   </details>
