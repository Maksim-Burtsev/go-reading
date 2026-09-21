# 03-ratelimiter

An importable per-key rate limiting library (token bucket and sliding window counter, in-memory
with TTL eviction, HTTP middleware) and a demo API server that uses it.

## Run

```sh
ALGORITHM=token_bucket LIMIT=5 PERIOD=1s BURST=10 go run ./apps/03-ratelimiter/cmd/ratelimiter  # then: curl -i localhost:8080/api/proverbs/1
```

## Where to start

1. `cmd/ratelimiter/main.go:run` — wiring and lifecycle: config, limiter choice, server, graceful shutdown.
2. `ratelimit/http.go:ServeHTTP` — how a limiter decision becomes headers, a 429, or a pass-through.
3. `ratelimit/store.go:do` — the shared per-key state, its locking, and the eviction loop below it.
4. `ratelimit/tokenbucket.go:take` — the token bucket: refill with integer time arithmetic, then take.
5. `ratelimit/slidingwindow.go:hit` — the sliding window counter: roll windows, estimate, count.

## Data flow

1. `run` loads `config.Config` from the environment and `newLimiter` builds a `TokenBucket` or a `SlidingWindow`.
2. `server.New` routes `/healthz` directly and wraps everything under `/api/` in `ratelimit.Middleware`.
3. The middleware derives the key with a `KeyFunc`: `RemoteIP`, or `ForwardedFor` when `TRUSTED_PROXIES` is set.
4. `Limiter.Allow` calls `store.do`, which locks the store, loads or creates the key's state, stamps `lastSeen` and runs the algorithm callback (`take` or `hit`) to get a `Decision`.
5. The middleware writes the `X-RateLimit-*` headers; a rejected request gets 429 with `Retry-After` and a JSON body, an allowed one reaches the proverbs handler.
6. In the background `store.evictLoop` wakes on every tick and deletes keys idle for at least the TTL.
7. On SIGTERM the context is canceled, `srv.Shutdown` drains in-flight requests, and the deferred `limiter.Close` stops the eviction goroutine.

## Go specifics here

1. `ratelimit/tokenbucket.go:9` — assignment to the blank identifier at package level.
   <details><summary>Explanation</summary>

   `var _ Limiter = (*TokenBucket)(nil)` is a compile-time check. Go interfaces are satisfied
   implicitly: nothing in `TokenBucket` says "implements Limiter". Converting a typed nil pointer
   to the interface forces the compiler to verify the method set, and `_` discards the value, so
   the line costs nothing at run time. If `Allow` ever changes signature, the build breaks here
   instead of at some distant call site.
   </details>

2. `ratelimit/http.go:123` — ranging over a function call.
   <details><summary>Explanation</summary>

   `slices.Backward(hops)` returns an iterator, `iter.Seq2[int, string]`: a function that takes a
   `yield` callback. Since Go 1.23 `for ... range` accepts such functions, much like a Python
   generator, and the loop body becomes the callback. Here it walks `X-Forwarded-For` from the last
   hop to the first without copying or reversing the slice, and `return` inside the loop stops the
   iteration.
   </details>

3. `ratelimit/store.go:96` — closing a channel that nothing is ever sent on.
   <details><summary>Explanation</summary>

   `stop` carries no values; closing it is a broadcast. A receive from a closed channel succeeds
   immediately, so the `case <-s.stop` branch in `evictLoop` fires and the goroutine returns. Its
   `defer close(s.done)` then unblocks `<-s.done` in `close`, which is how `Close` waits for the
   goroutine to exit before it clears the map. `sync.Once` makes a second `Close` a no-op instead of
   a panic from closing an already closed channel.
   </details>

4. `ratelimit/http.go:68` — the header name on the wire is not the one in the source.
   <details><summary>Explanation</summary>

   `http.Header.Set` canonicalizes keys with `textproto.CanonicalMIMEHeaderKey`, so
   `X-RateLimit-Limit` is stored and sent as `X-Ratelimit-Limit`. HTTP header names are
   case-insensitive, so clients do not care, but `curl -i` shows the canonical form and a test that
   indexes the `http.Header` map directly with the original spelling finds nothing. `Header.Get`
   canonicalizes too, which is why the tests use it.
   </details>

5. `cmd/ratelimiter/main.go:81` — deriving the shutdown context from a context that is already canceled.
   <details><summary>Explanation</summary>

   By the time shutdown starts, `ctx` is canceled: that is what triggered it. A timeout derived
   from it would be expired at once and `Shutdown` would not wait for anything.
   `context.WithoutCancel` keeps the parent's values but drops its cancellation, and
   `WithTimeout` then gives the drain its own deadline. Using `context.Background()` would also
   work, but it cuts the chain of values and the `contextcheck` linter flags it.
   </details>

## Questions

1. Why does `store.do` read `s.clock.Now()` after taking the mutex rather than before it?
   <details><summary>Answer</summary>

   So that the timestamps applied to one key never go backwards. If two goroutines read the clock
   first and then raced for the lock, the later timestamp could be applied first, and the next call
   would carry a time before the state's own. For the sliding window that time can fall into the
   previous window: `advance` sees a gap that is neither zero nor one period, zeroes both counters,
   and hands the key a fresh limit. The token bucket would place `refilledAt` in the future of `now`
   and report inflated reset and retry times. Reading the clock inside the critical section orders
   the timestamps the same way as the state updates.
   </details>

2. Neither limiter lets the caller configure the eviction TTL: it is `burst * interval` for the token bucket and `2 * period` for the sliding window. Why those values, and what would a shorter TTL break?
   <details><summary>Answer</summary>

   After that much idle time the state is indistinguishable from a fresh one: the bucket has
   refilled to `burst`, and both window counters have rolled out. Evicting it changes nothing a
   client can observe. With a shorter TTL a partially drained bucket, or a window still holding
   requests, would be deleted and recreated full, and a client that pauses just long enough would
   get more than its limit.
   </details>

3. `newLimiter` returns `ratelimit.NewTokenBucket(...)` directly, although that function returns `(*TokenBucket, error)` and `newLimiter` returns `(limiter, error)`. When construction fails, is the returned `limiter` equal to `nil`? Why is the code still correct?
   <details><summary>Answer</summary>

   It is not `nil`. The `*TokenBucket` nil pointer is converted to the `limiter` interface, and an
   interface holding a typed nil pointer compares unequal to `nil`, because it carries a type. The
   code is still correct because `run` checks `err` first and never touches the limiter on error:
   by Go convention the other results of a function are meaningless when `err != nil`. A caller
   that checked `limiter != nil` instead would be wrong.
   </details>

4. `ForwardedFor` reads `X-Forwarded-For` only when the direct peer is a trusted proxy, and reads it from right to left. What goes wrong if either rule is dropped?
   <details><summary>Answer</summary>

   Clients control the header's initial content; each proxy only appends the address it received
   the request from. Trusting the header from any peer lets a client send a fresh made-up address on
   every request and never hit its limit. Reading from the left has the same flaw behind a real
   proxy: the leftmost entries are whatever the client put there. Walking from the right and skipping
   known proxies stops at the first address that a trusted proxy itself observed.
   </details>

5. In the tests, `fakeTicker.tickAndWait` sends on the ticker channel twice. What does the second send buy, given that the channel is unbuffered?
   <details><summary>Answer</summary>

   A send on an unbuffered channel completes only when a receiver takes the value. The first send
   returns as soon as `evictLoop` receives the tick, while the eviction pass may still be running.
   The loop can receive the second tick only after `evictIdle` has returned and it is back in
   `select`, so when the second send returns the first pass is complete. The tests assert on the
   map without sleeping or polling.
   </details>
