# 06 · pg-service

A JSON API for users and their orders, backed by Postgres. Placing an order writes the order and its items in one transaction and lets Postgres compute the total; a user's orders are listed newest first, a page at a time, behind an opaque cursor. It runs on chi, pgx with sqlc-generated queries, and goose migrations applied at startup.

## Run it

```sh
docker compose -f apps/06-pg-service/docker-compose.yml up -d --wait
go run ./apps/06-pg-service/cmd/pg-service

# in a second terminal
curl -i -X POST localhost:8080/users -d '{"email":"ann@example.com","name":"Ann"}'
curl -i -X POST localhost:8080/users/1/orders -d '{"items":[{"sku":"BOOK-1","quantity":2,"unit_price_cents":1999}]}'
curl -i 'localhost:8080/users/1/orders?limit=1'
```

Repeat the first request to get a 409. Once the user has two orders, the list returns `next_cursor`; pass it back as `&cursor=` to get the next page.

## Questions

Answer from the code first, then open the answer.

1. `Migrate` closes its temporary `*sql.DB` in a deferred closure that assigns to `err`. What would a plain `defer sqlDB.Close()` lose, and why does the closure work only because the results are named?

   <details><summary>Answer</summary>

   A deferred call runs after the `return` statement has stored the results and before the caller sees them. Because `err` is a named result, the closure can read and overwrite it: `errors.Join(err, sqlDB.Close())` keeps the migration error and appends a close error, and since `errors.Join` drops nil arguments, a clean close changes nothing. `defer sqlDB.Close()` would throw the close error away, and with unnamed results no deferred code can change what the function returns. Closing this `*sql.DB` does not close the pgx pool it wraps, which the handlers go on using. Rule: when a cleanup can fail and the failure matters, name the error result and join the cleanup error into it in the defer.

   </details>

2. `srv.Serve` runs in its own goroutine and reports through `serveErr`, made with `make(chan error, 1)`. If the channel were unbuffered and `srv.Shutdown` returned an error, what would happen to that goroutine?

   <details><summary>Answer</summary>

   `run` returns right after the failed `Shutdown` and never receives from `serveErr`. `Serve` still returns `http.ErrServerClosed`, because `Shutdown` closes the listener first, and the goroutine then blocks forever on a send that nobody will receive. In `main` the process exits anyway, but a test or any other caller that runs `run` in-process leaks a goroutine per call. With capacity one the single send always completes, whether `run` reads it in the `select`, after `Shutdown`, or never. Rule: a goroutine that delivers exactly one result sends it on a channel with capacity one, so it can exit even when nobody is listening any more.

   </details>

3. SIGTERM arrives while a request is being served. Why is the shutdown deadline built on `context.WithoutCancel(ctx)` and not on `ctx`, and what happens to a request still stuck in a query when the deadline passes?

   <details><summary>Answer</summary>

   `ctx` comes from `signal.NotifyContext`, so it is already cancelled when shutdown starts. A timeout derived from it is done at once: `srv.Shutdown` would close the listener and the idle connections and return `context.Canceled` without waiting for active requests. `WithoutCancel` keeps the parent's values but drops its cancellation, so the fresh timeout governs the drain. `Shutdown` only waits and never cancels a running handler, so when it gives up, `run` calls `srv.Close()`: closing a connection cancels the context of the request on it, pgx abandons the query, and the deferred `pool.Close()`, which waits for every checked-out connection, can return. Rule: derive cleanup deadlines from a context detached from the one that triggered the cleanup, and force-close whatever misses them.

   </details>

4. A handler panics. What status does the client get, and what does the request log line show? What changes if `middleware.Recoverer` is moved to the front of the `r.Use` list?

   <details><summary>Answer</summary>

   chi applies `r.Use(a, b, c, d)` as `a(b(c(d(router))))`, so the first middleware is the outermost. `Recoverer` sits inside `logRequests`: it recovers the panic and writes 500 into the wrapped `ResponseWriter`, and `logRequests` then logs the request with status 500, its route and request ID. With `Recoverer` first, the panic would unwind through `logRequests`, whose logging runs after `next.ServeHTTP` returns and is not deferred, so the request would not be logged at all; the client would still get 500. `RequestID` and the client-IP middleware come first because `logRequests` and the handlers read what they put in the context. Rule: middleware that must observe the outcome, such as logging and metrics, wraps the middleware that produces it.

   </details>

5. A user with no orders lists them and gets `{"orders":[]}`. What would the body be if `newOrderPageResponse` started from `var resp orderPageResponse`, and why is `items` absent from list responses?

   <details><summary>Answer</summary>

   encoding/json writes a nil slice as `null` and an empty non-nil slice as `[]`. `make([]orderResponse, 0, len(p.Orders))` is non-nil even at length zero, so clients always get an array; a zero-value struct would leave `Orders` nil and produce `"orders":null`, which breaks clients that iterate without a null check. `items` carries `omitempty`, which skips nil and empty slices alike, and the list query never loads items, so the key is left out; `next_cursor` disappears the same way on the last page. Rule: nil and empty slices behave the same in Go code but not on the wire, so decide per field whether "nothing" is `[]`, `null` or an absent key.

   </details>

6. Two requests register the same email at the same moment. Why does `CreateUser` insert and inspect the error instead of checking with a `SELECT` first, and how does the handler end up answering 409?

   <details><summary>Answer</summary>

   A `SELECT` followed by an `INSERT` is check-then-act: both requests can see no row and both insert. The unique constraint is the only atomic check, so one insert wins and the other fails with SQLSTATE 23505. `errors.As(err, &pgErr)` takes a `**pgconn.PgError`, walks the wrap chain and stores the first `*pgconn.PgError` it finds, the Go form of psycopg's `except UniqueViolation`. Checking `ConstraintName` as well as the code keeps a later unique index on another column from being reported as a taken email. The store returns the sentinel `ErrEmailTaken`, the handler's `errors.Is` maps it to 409, and the HTTP layer never sees a pgx type. Rule: let the database enforce uniqueness, translate the violation into a domain error at the storage boundary, and map domain errors to status codes in one place.

   </details>

7. `CreateOrder` is called directly (the handler would reject this input) with two items, the second with `quantity: 0`. What happens to the order row inserted a moment earlier, and how does the caller learn that the order was invalid?

   <details><summary>Answer</summary>

   The items go in with a single `COPY`, which fails as a whole with SQLSTATE 23514 from the `quantity > 0` check. `pgx.BeginFunc` commits only when the closure returns nil and rolls back when it returns an error or panics, so the `orders` row goes away with the failed `COPY`. `CreateOrder` finds the `*pgconn.PgError` and returns `fmt.Errorf("%w: %w", ErrInvalidOrder, err)`: with two `%w` verbs, `errors.Is` matches both the sentinel and the Postgres error, and the handler answers 422. On success the closure hands the order out by assigning to the `order` declared outside it; Go closures capture variables, not values, so nothing like Python's `nonlocal` is needed. Rule: put multi-statement writes in a function-scoped transaction that commits only on a nil error, so every early return is a rollback.

   </details>

8. Two orders share a `created_at` and fall on the boundary between two pages. Why does `ListOrders` compare `(created_at, id)` rather than `created_at` alone, why does it fetch `limit + 1` rows, and why does the cursor keep microseconds?

   <details><summary>Answer</summary>

   `created_at` defaults to `now()`, the start time of the inserting transaction, so it is not unique. A cursor on the timestamp alone would skip the other order with `created_at < $2` or repeat the last one with `<=`. The row comparison `(created_at, id) < ($2, $3)` means "strictly after this row" in the `created_at DESC, id DESC` order, and the `(user_id, created_at DESC, id DESC)` index serves it. The extra row answers "is there another page?" without a `COUNT(*)`: it is dropped, and the cursor is built from the last row returned. `timestamptz` stores microseconds, so `UnixMicro` round-trips exactly; a millisecond cursor would sit below the real value and skip the rows in between. Rule: keyset pagination sorts and compares on a unique tuple, and its cursor keeps the key's full precision.

   </details>
