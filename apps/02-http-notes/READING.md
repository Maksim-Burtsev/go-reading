# 02 · http-notes

A JSON API for notes on plain `net/http`: create, list, read, replace and delete notes kept in
memory. Every response carries an `X-Request-ID`, every error comes back as a JSON envelope with a
machine-readable code, a request that runs too long gets a 503, and SIGINT or SIGTERM lets requests
in flight finish before the process exits. Bodies are checked with `go-playground/validator`, and
settings come from environment variables through `caarlos0/env`.

## Run it

```sh
go run ./apps/02-http-notes/cmd/http-notes
# in another terminal:
curl -si -d '{"title":"groceries","tags":["home"]}' localhost:8080/notes
curl -s localhost:8080/notes
curl -si -X PATCH localhost:8080/notes/42
curl -s -d '{"tags":["home",""]}' localhost:8080/notes
curl -si -H 'X-Request-ID: demo-1' localhost:8080/notes/missing
```

Stop the server with Ctrl-C to see the shutdown log lines. Every field of `config` can be set from
the environment.

## Questions

Answer from the code first, then open the answer.

1. SIGTERM arrives while a client is still uploading a request body. Is that request's context
   cancelled? And why does `run` build the shutdown context from `context.WithoutCancel(ctx)`
   rather than from `ctx`?

   <details><summary>Answer</summary>

   No. `signal.NotifyContext` cancels only `run`'s context; request contexts come from the server's
   `BaseContext`, `context.Background()` by default, so the signal just ends the `select` in `run`.
   `srv.Shutdown` then closes the listener and idle connections and waits for active requests until
   its own context is done. By then `ctx` is already cancelled, and a timeout derived from it would
   be born cancelled: `Shutdown` would return `context.Canceled` at once, `run` would fail, and the
   process would exit 1 in the middle of that upload. `WithoutCancel` keeps the values of `ctx` but
   not its cancellation, and `WithTimeout` gives the drain its own `SHUTDOWN_TIMEOUT`. Rule: cleanup
   after cancellation needs a context detached from the cancelled one, with its own deadline.

   </details>

2. `loadConfig` requires `HANDLER_TIMEOUT` to be shorter than `WRITE_TIMEOUT`. Suppose that check
   were gone and a request ran past `WRITE_TIMEOUT`. What would the client receive, and what would
   the access log say?

   <details><summary>Answer</summary>

   `WriteTimeout` is not a request timeout. It sets a deadline on the connection once the request
   headers are read; after it every write to that connection fails, while the handler keeps running.
   `http.TimeoutHandler` buffers the response and writes it only when the handler returns or its own
   timer fires, so with a longer handler timeout the 503, or a late success, is written after the
   deadline. The client sees the connection close with no response at all (Go's client reports
   `EOF`), while `logRequests`, which only sees the status and bytes handed to the writer, logs a
   503 or 200 that never arrived. Rule: server timeouts are connection deadlines that fail I/O;
   bound the work itself with a context or a `TimeoutHandler` that fires first.

   </details>

3. `NewHandler` applies `recoverPanics` first, then `withTimeout`, `logRequests` and
   `withRequestID`, so a request passes through them in the opposite order. A handler panics. Why
   must `recoverPanics` sit inside `withTimeout`, and what would the client get with no
   `recoverPanics` at all?

   <details><summary>Answer</summary>

   `recover` stops a panic only when a deferred function calls it directly, in the goroutine that
   is panicking; that is why `recoverPanic` itself is the deferred call. `http.TimeoutHandler` runs
   the wrapped handler in a goroutine of its own. Inside it, `recoverPanic` logs the original stack
   and its 500 lands in the timeout buffer like any response. `TimeoutHandler` also recovers in its
   goroutine and panics again in the serving one, so a recover placed outside would still answer 500
   but log the stack of that second panic. With none, `net/http` recovers per connection, logs
   `http: panic serving ...` and closes it: the client gets no response and `logRequests` logs
   nothing. Rule: a panic can be recovered only in its own goroutine, so recover where the work runs.

   </details>

4. `statusRecorder` embeds `http.ResponseWriter`, and `requestIDHandler` embeds `slog.Handler`.
   What does embedding give each of them for free, and why does `requestIDHandler` still define
   `WithAttrs` and `WithGroup`, which the embedded handler already has?

   <details><summary>Answer</summary>

   Embedding promotes the inner value's methods: `statusRecorder` takes `Header` from the real
   writer and overrides only `WriteHeader` and `Write`; `requestIDHandler` takes `Enabled` and
   overrides `Handle`. Promotion is not inheritance: a promoted method runs on the inner value and
   returns what it returns. `logger.With(...)` calls `WithAttrs`, and the promoted one would hand
   back the bare JSON handler, so records logged through the derived logger would silently lose
   `request_id`; the overrides re-wrap the result. Promotion also stops at the field's static type:
   the writer's `Flush` is hidden behind `statusRecorder`, hence its `Unwrap`. Rule: a wrapper that
   embeds an interface must override every method that returns a new instance of that interface.

   </details>

5. `Store.Get` returns `fmt.Errorf("get note %q: %w", id, ErrNotFound)`, and a malformed body
   produces an `*httpError`. How does `writeError` turn each into a status code, and what would
   the client get if the store wrapped with `%v` instead of `%w`?

   <details><summary>Answer</summary>

   `%w` makes the new error wrap the old one: the text reads the same, and the cause stays reachable
   through `Unwrap`, which `errors.Is` and `errors.As` follow down the chain. `writeError` checks a
   sentinel with `errors.Is(err, notes.ErrNotFound)`, a comparison with one package-level value, and
   a typed error with `errors.As(err, &httpErr)`, which finds the first `*httpError` in the chain and
   hands it back with its status and code. With `%v` the text would not change but the chain would
   be cut, nothing would match, and a missing note would fall through to `default`: an error log and
   a 500 instead of a 404. Rule: wrap with `%w` when callers may need the cause, match with
   `errors.Is` or `errors.As`, and keep matching on error text (as for unknown JSON fields) a last resort.

   </details>

6. `POST /notes` with `{"title":"a"} {"title":"b"}` gets a 400, but `{"title":"a"}` followed by a
   newline is accepted. What does the second `Decode` in `decodeJSON` check, and what would a
   single `Decode` let through?

   <details><summary>Answer</summary>

   `json.Decoder` reads a stream of JSON values, and one `Decode` consumes exactly one of them,
   leaving the rest of the body unread. A single call would accept the first object and silently
   ignore anything after it, a second object or plain garbage. Decoding again into an empty struct
   must return `io.EOF`, which happens only when nothing but whitespace is left. `MaxBytesReader`
   caps what the decoder may read, so a body over 64 KiB fails with `*http.MaxBytesError` and becomes
   a 413, and `DisallowUnknownFields` turns an unexpected key into an error instead of dropping it.
   `json.Unmarshal` rejects trailing data by itself but needs the whole body in memory first. Rule:
   treat `json.Decoder` as a stream reader; bound its input and check that the input ended.

   </details>

7. A note created without tags is stored with `Tags == nil`. Why does the API still answer
   `"tags": []`, and what would change if `newNoteResponse` passed `n.Tags` through unchanged?

   <details><summary>Answer</summary>

   `encoding/json` encodes a nil slice as `null` and an empty, non-nil slice as `[]`. The request had
   no `tags`, so the decoded field is nil, and `slices.Clone` keeps nil as nil, so the store holds
   nil. `newNoteResponse` swaps it for `[]string{}`; without that, clients would get `"tags": null`,
   and a Python client looping over `note["tags"]` would fail on `None`. `listNotes` starts from
   `make([]noteResponse, 0, len(all))` for the same reason, so an empty store answers
   `{"notes":[]}`. Inside Go the difference rarely matters: `len`, `range` and `append` treat a nil
   slice as empty. Rule: decide between `null` and `[]` explicitly wherever a slice crosses a JSON
   boundary.

   </details>

8. `Store.Update` reads `n, ok := s.notes[id]`, edits `n` and stores it back with
   `s.notes[id] = n`, and every method that returns a `Note` passes it through `clone`. Why not
   assign `s.notes[id].Title` directly, and why clone a value that is already a copy?

   <details><summary>Answer</summary>

   Indexing a map yields a copy of the element, and map elements are not addressable, so
   `s.notes[id].Title = in.Title` does not compile; `Update` edits the copy and stores it back while
   it still holds the lock. The copy is shallow: a slice field is a small header (pointer, length,
   capacity) pointing at a backing array the copy shares. Without `slices.Clone`, a caller's `Note`
   would share `Tags` with the stored one, and `note.Tags[0] = "x"` would change the store behind
   its mutex. No handler writes into `Tags` today, so nothing breaks yet; the clones are what keep
   the promise "callers never share memory with the store". Rule: a struct copy is shallow; slices,
   maps and pointers inside it share memory until you copy them.

   </details>
