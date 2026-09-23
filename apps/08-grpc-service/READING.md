# 08 · grpc-service

An inventory service on grpc-go whose API is a protobuf contract compiled with buf. Clients look up an
item, stream a filtered list of items, and reserve stock for several SKUs at once: every line or none,
and idempotent by reservation id. The server also answers standard health checks and reflection, logs
every call with its status code, and drains in-flight calls on SIGTERM.

## Run it

```sh
go run ./apps/08-grpc-service/cmd/inventory

# in a second terminal; reflection is registered, so grpcurl needs no .proto files
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext localhost:50051 list
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext -d '{"sku":"BOOK-GOPL"}' localhost:50051 inventory.v1.InventoryService/GetItem
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext -d '{"sku_prefix":"BOOK-","in_stock_only":true}' localhost:50051 inventory.v1.InventoryService/ListItems
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext -d '{"reservation_id":"order-1","lines":[{"sku":"MUG-GOPHER","quantity":2}]}' localhost:50051 inventory.v1.InventoryService/Reserve
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4 -plaintext -d '{"service":"inventory.v1.InventoryService"}' localhost:50051 grpc.health.v1.Health/Check

# regenerate the Go code from the proto; buf and its plugins run through go run
go generate ./apps/08-grpc-service/...
```

Send the same Reserve twice (same `create_time`), then again with `"quantity":3` (ALREADY_EXISTS). Stop
the server with ctrl+c and read its last log lines.

## Questions

Answer from the code first, then open the answer.

1. SIGTERM arrives while a `Reserve` call is running, and the call is still running when
   `INVENTORY_SHUTDOWN_TIMEOUT` expires. Follow `run` and `Server.Shutdown`: what does the client see,
   what does the process exit with, and why is the shutdown context built from `context.WithoutCancel(ctx)`?

   <details><summary>Answer</summary>

   The signal has cancelled `ctx`, so a child of it would be born cancelled: `context.WithoutCancel`
   keeps its values without the cancellation, and `WithTimeout` adds the budget. `GracefulStop` takes no
   context, so `Shutdown` races it against `ctx.Done()` in a goroutine. At the deadline, `Stop` closes
   the connections and cancels the handler: the client gets UNAVAILABLE, `run` returns
   `shutdown: graceful stop: context deadline exceeded`, and the exit status is 1. `Shutdown` still
   waits for `GracefulStop`, which ends only after the handler returns, so the budget holds only while
   handlers honour cancellation. The buffered `serveErr` lets the `Serve` goroutine exit unread.
   Rule: Race a call that takes no context against `ctx.Done()`, with a forced fallback.

   </details>

2. `GetItem` panics inside the store. What does the client receive, what is logged, and what changes if
   `unaryLogging` and `unaryRecovery` swap places in `grpc.ChainUnaryInterceptor`?

   <details><summary>Answer</summary>

   The closure in `unaryRecovery` has named results `(resp any, err error)`. Its deferred function calls
   `recover()`, which stops the panic, then assigns `err`. Deferred calls run as the function exits, by
   return or by panic, before the caller sees the results, so they can replace them. The client gets
   INTERNAL with the fixed text `internal error`, and the panic value and stack only reach the log. The
   first interceptor in the chain is the outermost, so `unaryLogging` sees that status and logs
   `rpc failed` at ERROR. Swapped, the panic unwinds through `unaryLogging` before anything recovers it,
   and `logCall` never runs. Rule: Interceptors nest in the order given, like HTTP middleware, so
   recovery goes inside everything that must observe the final status.

   </details>

3. One client calls `GetItem` with no deadline, another with a 1-second deadline, a third with a
   1-minute deadline. What deadline does the handler's context carry in each case, and why does the
   default timeout apply to unary calls only?

   <details><summary>Answer</summary>

   grpc-go sends the client's deadline in the `grpc-timeout` header and gives the handler a context that
   carries it, so the second and third handlers see 1 s and 1 min. `unaryDefaultTimeout` adds
   `context.WithTimeout` only when `ctx.Deadline()` reports none, so the first gets
   `INVENTORY_DEFAULT_TIMEOUT` (5 s). It is a fallback, not a cap. Streams get no default because the
   stream chain also serves the health `Watch` and reflection streams, which stay open for as long as
   the client wants. Rule: A gRPC deadline travels with the request through `ctx` on every hop, and a
   server-side default only covers clients that did not set one.

   </details>

4. `service` embeds `inventoryv1.UnimplementedInventoryServiceServer` and declares `GetItem`,
   `ListItems` and `Reserve` itself. What stops you from deleting the embedded field, and what does a
   client get from an RPC that is added to the proto and regenerated but not implemented yet?

   <details><summary>Answer</summary>

   The generated `InventoryServiceServer` interface has the unexported method
   `mustEmbedUnimplementedInventoryServiceServer`, which only the generated package can declare, so
   other types satisfy it only by embedding one of that package's types; without the field, registration
   does not compile. Embedding promotes every method. The ones `service` declares are shallower and win,
   and a new RPC falls through to the promoted stub, which returns UNIMPLEMENTED. The test fake
   `panickingInventory` does the same with an interface: it embeds a nil `Inventory`, compiles with only
   `Get` and `List`, and panics if `Reserve` is called. Rule: An unexported method seals an interface to
   its package, which is how generated gRPC code keeps servers forward compatible.

   </details>

5. The store fails a reservation with `fmt.Errorf("reserve %s: sku %s: requested %d, available %d: %w", ..., ErrInsufficientStock)`.
   What does the client receive? What would it receive if `Reserve` returned that error without
   `toStatus`? And why does `ListItems` wrap a failed `Send` with `%w` instead of passing it through
   `toStatus`?

   <details><summary>Answer</summary>

   `toStatus` finds the sentinel through the `%w` chain with `errors.Is` and returns FAILED_PRECONDITION
   with the full message. A plain handler error reaches the client as UNKNOWN (grpc-go only understands
   status and context errors) and is logged at ERROR. A failed `Send` already carries a status such as
   CANCELED, and `status.Code` looks through `%w` to find it, so a disconnect is logged at INFO.
   `toStatus` would miss it, since a status error does not match `errors.Is(err, context.Canceled)`, and
   would report INTERNAL. Rule: Handlers return status errors, domain errors are mapped to codes once at
   the boundary, and errors that already carry a status pass through wrapped, as with
   `HTTPException(409)` versus a bare exception that becomes a 500.

   </details>

6. A client calls `ListItems` on a large catalog, reads one message and then stops reading without
   cancelling. What keeps the handler running, and what finally ends it? What happens if the client
   cancels instead?

   <details><summary>Answer</summary>

   `stream.Send` blocks once the stream's HTTP/2 flow-control window is full, and the window reopens
   only as the client reads. The limit bounds the work, not the time, and the `ctx.Err()` check between
   sends cannot run while `Send` is blocked. Streams get no default timeout, so the handler waits until
   the client cancels, its deadline passes, or the connection closes (during shutdown, `Stop` does that
   and the call is logged at ERROR as UNAVAILABLE). On a client cancel, the blocked `Send` returns
   CANCELED and the call is logged at INFO. The five seeded items fit in the window, so this needs a
   real catalog to show. Rule: A server stream lives as long as the client lets it, so bound it with
   deadlines or server-side limits, and treat `Send` as a blocking call.

   </details>

7. `Store.Reserve` reads `item := s.items[line.SKU]`, changes it and writes it back, and it returns
   `r.clone()` rather than `r`. What breaks if the write-back or the clone is dropped?

   <details><summary>Answer</summary>

   `s.items` is a `map[string]Item` of struct values. Indexing a map yields a copy, and map elements are
   not addressable, so `s.items[sku].Available -= n` does not compile. Without the write-back, the
   change stays in the copy and stock never moves; the same copying keeps what `Get` and `List` return
   detached from the store. A `Reservation` is copied by value too, but copying its `Lines` slice copies
   only the header. Without `clone`, the caller and `s.reservations[id]` would share one backing array,
   so a caller editing a quantity would change the stored reservation outside the mutex: a data race
   that also breaks the next replay. Rule: Struct copies are shallow, so copy slices, maps and pointers
   where data crosses a lock or an API boundary.

   </details>

8. Two clients send `Reserve` at the same moment with the same `reservation_id` and the same lines in a
   different order. How many times is stock decremented, what does each client get, and what makes the
   replay comparison correct?

   <details><summary>Answer</summary>

   Once. `Reserve` holds `s.mu` across the whole sequence: look up the id, check every line, apply the
   changes, store the reservation. The second caller waits for the lock, finds the id, and gets the
   stored reservation back with OK and the original `create_time`. The comparison works because
   `normalize` sorts the lines by SKU and rejects duplicate SKUs before anything is compared, and
   because `Line` is a comparable struct, so `slices.Equal` compares lines field by field. If the lookup
   and the write were in separate critical sections, both callers could see no reservation and both
   could reserve. Rule: An idempotency check and the write it guards belong in one critical section or
   one database transaction, and they compare a canonical form of the request.

   </details>
