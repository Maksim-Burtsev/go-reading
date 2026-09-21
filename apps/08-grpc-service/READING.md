# 08-grpc-service

An inventory gRPC service: get an item, stream a filtered list, reserve stock atomically and idempotently.

## Run

```sh
go run ./apps/08-grpc-service/cmd/inventory
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext -d '{"sku":"BOOK-GOPL"}' localhost:50051 inventory.v1.InventoryService/GetItem
go generate ./apps/08-grpc-service/...   # buf lint + regenerate gen/ from proto/
```

## Where to start

1. `proto/inventory/v1/inventory.proto:InventoryService` — the contract: three RPCs and the status code each failure maps to.
2. `cmd/inventory/main.go:run` — config from env, listener, server start and the two-stage shutdown.
3. `internal/server/server.go:New` — the interceptor chains and what gets registered on the `grpc.Server`.
4. `internal/server/service.go:ListItems` — request validation, the server stream, and `toStatus` turning domain errors into codes.
5. `internal/inventory/store.go:Reserve` — the all-or-nothing, idempotent stock update behind a single mutex.

## Data flow

1. A client call arrives on the listener opened in `run` and is dispatched by `grpc.Server` to a method on `service`.
2. Unary calls pass through `unaryLogging` → `unaryRecovery` → `unaryDefaultTimeout`; streams through `streamLogging` → `streamRecovery`.
3. The `service` handler reads the generated request with `Get*` accessors, rejects a bad `sku` or `limit` itself and converts the rest to domain types; reservation input is validated by `normalize` in the store.
4. The handler calls the `Inventory` interface; in production that is `*inventory.Store`, which locks, checks `ctx.Err()` and reads or mutates its maps.
5. Domain sentinel errors come back wrapped; `toStatus` matches them with `errors.Is` and picks the gRPC code.
6. The result is converted to generated messages (`toProtoItem`, `toProtoReservation`) and returned, or sent item by item on the `ListItems` stream.
7. On the way out, `logCall` logs method, code and duration at info or error level depending on the code.

## Go specifics here

1. `internal/server/interceptors.go:56` — assignment to `err` inside a deferred function.
   <details><summary>Explanation</summary>

   The closure declares named results `(resp any, err error)`. Deferred functions run after the results are set (or while a panic unwinds) but before the caller sees them, so they can overwrite `err`. `recover()` stops a panic only when called directly by a deferred function, and after it the function returns normally with whatever its named results hold: `resp` stays nil and `err` becomes `codes.Internal`. Without named results the deferred function would have no way to change what the caller receives.
   </details>

2. `internal/inventory/store.go:165` — writing `item` back into the map.
   <details><summary>Explanation</summary>

   `s.items` is a `map[string]Item` holding struct values, not pointers. `item := s.items[line.SKU]` copies the struct, the next two lines change the copy, and this line stores it back. `s.items[sku].Available -= n` does not compile because map elements are not addressable. The upside is that `Get` and `List` return copies the caller cannot use to mutate the store.
   </details>

3. `internal/server/service.go:23` — a struct field with a type and no name.
   <details><summary>Explanation</summary>

   This is embedding. All methods of `UnimplementedInventoryServiceServer` are promoted to `service`, so it satisfies the generated `InventoryServiceServer` interface even for RPCs added to the proto later; those answer `codes.Unimplemented` until written. The generated code asks for embedding by value rather than by pointer: a nil embedded pointer would panic at registration. `service` then defines its own `GetItem`, `ListItems` and `Reserve`, which take precedence over the promoted ones.
   </details>

4. `cmd/inventory/main.go:64` — `select` with an empty `case`.
   <details><summary>Explanation</summary>

   `select` blocks until one of its channel operations can proceed. If `Serve` fails first, its error arrives on `serveErr` and `run` returns it. If a signal cancels `ctx` first, `<-ctx.Done()` becomes ready, the empty case body does nothing, and execution continues below the `select` into shutdown. `serveErr` is buffered with capacity 1 so the serving goroutine can always deliver its result and exit, even if nobody reads it.
   </details>

5. `internal/server/server_test.go:39` — a fake that defines only some of the interface's methods.
   <details><summary>Explanation</summary>

   `panickingInventory` embeds the `server.Inventory` interface, which is left nil. Embedding promotes all three interface methods, so the struct satisfies `server.Inventory` at compile time; the methods it defines itself (`Get`, `List`) shadow the promoted ones. Calling the one it does not define (`Reserve`) would dereference the nil interface and panic. It is a common way to write a partial test double without stubbing every method.
   </details>

## Questions

1. `GetItem` panics inside the store. Which interceptor turns it into an error, what code does the client get, and at what level does the logging interceptor log the call? Why does the order of arguments to `grpc.ChainUnaryInterceptor` matter here?
   <details><summary>Answer</summary>

   `unaryRecovery` recovers the panic, logs it with `debug.Stack()` and sets `err` to `status.Error(codes.Internal, "internal error")`, so the client gets `INTERNAL` without the panic text. The first interceptor in the chain is the outermost, so `unaryLogging` wraps `unaryRecovery`, sees the returned `Internal` status and logs "rpc failed" at error level. If the order were reversed, the panic would unwind through the logging interceptor before being recovered, `logCall` would never run, and the only trace of the call would be the panic log.
   </details>

2. Two clients call `Reserve` at the same moment with the same `reservation_id` and the same lines. How many times is stock decremented, and what does each client receive?
   <details><summary>Answer</summary>

   Once. `Store.Reserve` holds `s.mu` across the whole lookup-check-commit sequence, so the calls are serialized. The first finds no existing reservation, commits it and stores it in `s.reservations`. The second finds it, sees that the normalized lines are equal (`slices.Equal` on the SKU-sorted slices) and returns the stored reservation with OK and the same `create_time`. `TestStoreConcurrentReservations/same_id_reserves_once` checks exactly this with 100 goroutines under `-race`.
   </details>

3. `Store.Reserve` returns `r.clone()` instead of `r`. What could go wrong without the clone?
   <details><summary>Answer</summary>

   A `Reservation` value contains a slice, and copying the struct copies only the slice header, not the backing array. Without the clone, the caller and `s.reservations[id]` would share the same `[]Line` array: a caller modifying `r.Lines[0].Quantity` would silently change the stored reservation, outside the mutex, which is both a data race and a way to break the idempotency comparison on the next replay.
   </details>

4. The default timeout is applied only to unary calls. Why not to streams as well, and what bounds a `ListItems` call instead?
   <details><summary>Answer</summary>

   The same stream interceptor chain serves `grpc.health.v1.Health/Watch` and the reflection stream, which are meant to stay open for as long as the client wants; a default deadline would cut them off every few seconds. `ListItems` is bounded by the request itself: at most `maxListLimit` (1000) items from an in-memory snapshot. The handler still honours a client deadline or cancellation by checking `ctx.Err()` between sends.
   </details>

5. After SIGTERM, one `Reserve` call is still running when `INVENTORY_SHUTDOWN_TIMEOUT` expires. Walk through what happens and what the process exit code is.
   <details><summary>Answer</summary>

   `run` builds `shutdownCtx` from `context.WithoutCancel(ctx)` (the signal already cancelled `ctx`, so a child of it would be done immediately) and calls `Shutdown`. Health checks switch to `NOT_SERVING`, `GracefulStop` runs in a goroutine and waits for the call. When `shutdownCtx` expires, `Shutdown` calls `Stop`, which closes connections and cancels the in-flight call (the client sees `UNAVAILABLE`), then waits for `GracefulStop` to return and reports `graceful stop: context deadline exceeded`. `run` returns it wrapped as `shutdown: ...`, `main` prints it to stderr and exits with status 1. `TestShutdownCancelsCallsAfterTimeout` covers the server side of this.
   </details>
