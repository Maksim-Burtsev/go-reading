# 02-http-notes

A JSON CRUD API for notes on plain `net/http`: an in-memory store, request-id, logging, timeout and recover middleware, and graceful shutdown.

## Run

```sh
go run ./apps/02-http-notes/cmd/http-notes   # then: curl -d '{"title":"hello"}' localhost:8080/notes
```

## Where to start

1. `cmd/http-notes/main.go:run` — config from env, logger, `http.Server` timeouts, and the signal-driven graceful shutdown.
2. `internal/httpapi/handler.go:NewHandler` — the route table and the order of the middleware chain.
3. `internal/httpapi/notes.go:createNote` — a typical handler: decode, validate, call the store, respond.
4. `internal/httpapi/respond.go:writeError` — how every error becomes a status code and the JSON error envelope.
5. `internal/notes/store.go:Update` — the store: lock discipline, the sentinel error, and copy-out semantics.

## Data flow

1. `http.Server` accepts the connection, enforces the read/write timeouts and calls the handler built by `NewHandler`.
2. `withRequestID` takes `X-Request-ID` (or generates a UUID), puts it in the context and echoes it in the response.
3. `logRequests` swaps in a `statusRecorder` and logs method, path, status, size and duration once the chain returns.
4. `withTimeout` runs the rest in `http.TimeoutHandler`: the response is buffered and replaced with a 503 JSON body if the deadline passes.
5. `recoverPanics` turns a panic into an `ERROR` log with the stack and a 500 envelope.
6. `ServeMux` matches `METHOD /path/{id}`; `api.handle` adapts the error-returning handler and routes any error to `writeError`.
7. The handler decodes with `MaxBytesReader` and `DisallowUnknownFields`, validates with `validator`, and calls `notes.Store`.
8. The store works under a `sync.RWMutex` and hands back copies; `writeJSON` serialises the response DTO.

## Go specifics here

1. `internal/notes/store.go:128` — a value receiver that clones a field

   <details><summary>Explanation</summary>

   `n` is already a copy of the struct, but a struct copy only copies the slice header (pointer, length, capacity), not the array behind it. Without `slices.Clone`, a caller doing `note.Tags[0] = "x"` would write straight into the map-held value, outside the mutex. Python has the same trap with a list inside a dataclass; what makes it easy to miss in Go is that `n, ok := s.notes[id]` really does copy the struct, so the copy looks deep when it is not.
   </details>

2. `internal/httpapi/notes.go:36` — replacing a nil slice with an empty one

   <details><summary>Explanation</summary>

   `encoding/json` marshals a nil slice as `null` and an empty slice as `[]`. A note created without tags stores `nil` (because `slices.Clone(nil)` is `nil`), so without this branch clients would get `"tags": null`. The same reason is behind `make([]noteResponse, 0, len(all))` in `listNotes`.
   </details>

3. `internal/httpapi/requestid.go:61` — overriding methods that the embedded interface already provides

   <details><summary>Explanation</summary>

   Embedding `slog.Handler` promotes its methods, but promotion is not inheritance: the promoted `WithAttrs` returns the inner handler, not a `requestIDHandler`. Any `logger.With(...)` would then silently drop the wrapper and every record logged through it would lose `request_id`. Overriding `WithAttrs` and `WithGroup` re-wraps the result.
   </details>

4. `internal/httpapi/middleware.go:57` — deferring a method instead of a closure

   <details><summary>Explanation</summary>

   `recover()` only stops a panic when it is called directly by the deferred function. `recoverPanic` is the deferred function here, so calling `recover()` inside it works; moving that call one level deeper into a helper would return `nil` and the panic would propagate. Passing `w` and `r` as arguments also means they are evaluated when `defer` executes, not when the panic happens.
   </details>

5. `cmd/http-notes/main.go:80` — `context.WithoutCancel` under `WithTimeout`

   <details><summary>Explanation</summary>

   By this line `ctx` is done (SIGTERM arrived), and any context derived from it is born cancelled, so `srv.Shutdown` would return immediately. `context.WithoutCancel` keeps the values but detaches the cancellation, and `WithTimeout` then gives the drain its own budget.
   </details>

## Questions

1. Delete the `WithAttrs` method from `requestIDHandler`. Which assertion in `TestRequestID` fails, and why only that one?

   <details><summary>Answer</summary>

   The handler inside the test logs through `logger.With(slog.String("component", "test"))`. Without the override, `With` returns the bare `JSONHandler`, so that record has no `request_id` and the loop over records fails on the first one. The `http request` record is logged by the original logger, which still has the wrapper, so it keeps passing.
   </details>

2. Why is `recoverPanics` wrapped inside `withTimeout` and not outside it?

   <details><summary>Answer</summary>

   `http.TimeoutHandler` runs the inner handler in its own goroutine. A panic there is caught by `TimeoutHandler` and re-raised in the serving goroutine, so a recover placed outside would log the stack of the re-panic in `TimeoutHandler.ServeHTTP` instead of the handler that failed. Inside, the recover runs in the handler's goroutine with the original stack, and its 500 goes into the timeout buffer like any other response. `logRequests` stays outside both so it sees the final status, including 503 and 500.
   </details>

3. `PATCH /notes/42` returns a JSON 405 with `Allow: GET, PUT, DELETE`. Which pattern handles it, and what would the client get if that line were removed?

   <details><summary>Answer</summary>

   The method-less pattern `/notes/{id}` matches every method; the patterns with a method are more specific, so they win for GET, PUT and DELETE, and everything else falls through to `methodNotAllowed`. Without it the catch-all `/` would match `PATCH /notes/42` and answer with a JSON 404. `ServeMux` only produces its own plain-text 405 when no pattern at all matches the request, and `/` always matches.
   </details>

4. `List` unlocks by hand before sorting instead of using `defer` like every other method. Why sort at all, and why is sorting outside the lock safe?

   <details><summary>Answer</summary>

   Go randomises map iteration order, so without sorting two identical requests could list notes in different orders. `out` holds clones built while the read lock was held, so nothing else can reach it; sorting it after `RUnlock` shortens the time writers wait.
   </details>

5. A request is in flight when SIGTERM arrives. Is its context cancelled, and what bounds how long the process waits for it?

   <details><summary>Answer</summary>

   No. `signal.NotifyContext` cancels only `run`'s context, and the server does not derive request contexts from it (no `BaseContext`). `srv.Shutdown` closes the listener, closes idle keep-alive connections and waits for active ones, bounded by `SHUTDOWN_TIMEOUT`. Each request is also bounded by `HANDLER_TIMEOUT`, which `loadConfig` requires to be shorter than `WRITE_TIMEOUT`. If the shutdown budget runs out, `Shutdown` returns `context.DeadlineExceeded`, `run` returns it and `main` exits with status 1.
   </details>
