# 05 · lru-cache

A read-through HTTP caching proxy in front of one upstream, proxy.golang.org by default. `GET`
requests are answered from a generic in-memory LRU cache with a TTL; misses go upstream, with
concurrent misses for the same URL collapsed into one request by singleflight, and only 200
responses are kept. `GET /_cache/stats` reports the counters and `PURGE /path` drops an entry.

## Run it

```sh
LOG_LEVEL=debug CACHE_SIZE=2 go run ./apps/05-lru-cache/cmd/lru-cache

# in another terminal
for i in 1 2; do curl -si localhost:8080/golang.org/x/sync/@v/list | grep X-Cache; done
curl -s localhost:8080/golang.org/x/text/@v/list > /dev/null
curl -s localhost:8080/golang.org/x/net/@v/list > /dev/null
curl -s localhost:8080/_cache/stats
go test -run '^$' -bench . ./apps/05-lru-cache/lru
```

The first request is a MISS and the second a HIT; with `CACHE_SIZE=2` the third URL evicts the
first, which the debug log reports. The upstream is on the internet, so this needs network access.

## Questions

Answer from the code first, then open the answer.

1. `run` writes `lru.WithTTL[string, *proxy.Response](cfg.CacheTTL)` but a bare
   `lru.WithOnEvict(...)`. Why the difference, and would
   `lru.New[string, *proxy.Response](cfg.CacheSize, lru.WithTTL(cfg.CacheTTL))` compile?

   <details><summary>Answer</summary>

   Go infers a call's type arguments from that call's own arguments; the type its result is later
   passed to plays no part. `WithOnEvict` receives a
   `func(string, *proxy.Response, lru.EvictReason)`, which pins both `K` and `V`. `WithTTL` receives
   only a `time.Duration`, which mentions neither, so the compiler stops with "in call to
   lru.WithTTL, cannot infer K", even when `New` is instantiated explicitly, because `WithTTL(...)`
   is a separate call checked on its own. `New` then infers its own `K` and `V` from the options it
   is given. Rule: a type parameter that appears only in a function's result must be spelled out at
   every call site, so APIs avoid generic helpers whose arguments do not mention it.

   </details>

2. SIGTERM arrives. Which goroutines does `g.Wait()` wait for, what makes each of them return, and
   what changes if `srv.Serve` fails on its own first?

   <details><summary>Answer</summary>

   `signal.NotifyContext` cancels `ctx` and with it `gctx`, its child from `errgroup.WithContext`.
   The shutdown goroutine wakes from `<-gctx.Done()` and calls `srv.Shutdown` with a fresh timeout
   built on `context.WithoutCancel(gctx)`; `Serve` returns `http.ErrServerClosed` as soon as
   `Shutdown` closes the listener, and that is mapped to `nil`; `sweep`, started only when
   `CACHE_TTL` is positive, returns from its `select`. If `Serve` fails first, its goroutine returns
   an error, the errgroup cancels `gctx`, the other two exit the same way, `g.Wait()` returns that
   first error and `main` exits with status 1. Rule: under `errgroup.WithContext` a failed sibling
   and a canceled parent look the same, so every goroutine must watch the group's context.

   </details>

3. `proxy.Cache` is declared as `type Cache = lru.Cache[string, *Response]`. What breaks if the `=`
   is removed?

   <details><summary>Answer</summary>

   With `=`, `Cache` is an alias, a second name for exactly the same type, so `main` passes its
   `*lru.Cache[string, *proxy.Response]` to `proxy.New` and the proxy calls `Get`, `Peek` and `Set`
   on it. Without `=`, `Cache` becomes a new defined type with the same underlying struct but an
   empty method set, because methods belong to the type they were declared on. Every
   `p.cache.Get(...)` stops compiling ("type *Cache has no field or method Get"), and `main` could
   pass its cache in only through an explicit conversion. Rule: `type A = B` gives an existing type
   another name; `type A B` creates a new type with B's structure but none of its methods.

   </details>

4. A client asks for an uncached URL and disconnects after 100 ms; the upstream takes 2 s, and two
   other clients are waiting for the same URL. What happens to the fetch, to the other two, and to
   the cache?

   <details><summary>Answer</summary>

   All three calls to `load` joined one singleflight flight, and each waits in its own `select` on
   its request's `ctx.Done()` and on the channel `DoChan` returned to it. The first client's context
   is canceled, its `select` takes the `ctx.Done()` branch, and `writeError` only logs that the
   client left. The fetch runs on `context.WithoutCancel(ctx)`, so the requester that started it
   leaving does not cancel it; only the HTTP client's `Timeout` bounds it. After 2 s the other two
   get the same `*Response` and a 200 is stored. Each `DoChan` channel holds one result, so the
   flight never blocks on a caller that left. Rule: work shared by several requests must not run on
   any one request's context; detach it and give it its own bound.

   </details>

5. SIGTERM arrives while such a fetch is still running and every client that wanted it has gone.
   Does `srv.Shutdown` wait for it?

   <details><summary>Answer</summary>

   No. `Shutdown` closes the listeners and waits until every connection is idle, that is, until the
   handlers have returned. The handlers for that URL returned when their clients left, and the fetch
   runs in a goroutine that singleflight started and nothing tracks. So `Shutdown` returns at once,
   `g.Wait()` and `run` return, and when `main` returns the process exits with the fetch cut off.
   Here that only loses one cache fill. Rule: `http.Server.Shutdown` drains connections, not the
   goroutines your handlers started; background work that must finish needs its own `WaitGroup` or
   errgroup, waited on after `Shutdown`.

   </details>

6. Inside the function passed to `DoChan`, `load` calls `p.cache.Peek(key)` before fetching,
   although `handleGet` has just missed. Which race does that close, and why `Peek` and not `Get`?

   <details><summary>Answer</summary>

   Between `handleGet`'s miss and its `DoChan` call, an earlier flight for the same key can store
   its response and finish, and singleflight forgets a key as soon as its function returns. The late
   request then starts a second flight, and the re-check turns it into a cache read instead of a
   second upstream fetch: the check-then-act gap is closed by checking again inside the section that
   serializes fills. `Peek` leaves the counters and recency alone, while `Get` would record a second
   hit or miss for a request that `handleGet` has already counted and skew `hit_ratio`. Rule: when a
   miss triggers an expensive fill, re-check the cache inside whatever serializes the fill.

   </details>

7. `Response.serve` copies every header slice with `slices.Clone` before putting it into
   `w.Header()`, yet writes `resp.body` as is. Why the difference?

   <details><summary>Answer</summary>

   A cached `*Response` is shared by every request that hits it, concurrently and without a lock,
   which is safe only while nobody changes it. A slice is a view of a backing array, so
   `h[name] = values` would make each response's header point at the cache's array; any code that
   later changed a value in place, say a middleware rewriting `h["Content-Type"][0]`, would race
   with other requests and corrupt the cached entry. The body needs no copy because the `io.Writer`
   contract forbids `Write` from modifying the slice it is given. Rule: data shared across
   goroutines without a lock has to stay immutable, so copy slices and maps before handing them to
   code that might modify them.

   </details>

8. `Get`, `Set`, `Remove`, `Purge` and `DeleteExpired` call `c.mu.Unlock()` explicitly and then
   `c.notify(evicted)`, while `Peek`, `Len` and `Stats` use `defer`. What would `defer` break in the
   first group, and what does the explicit unlock cost?

   <details><summary>Answer</summary>

   `notify` runs the user's `OnEvict`, which may call back into the cache. `sync.Mutex` is not
   reentrant, unlike Python's `threading.RLock`: a goroutine that locks a mutex it already holds
   blocks forever. With `defer`, the unlock would run only when the method returns, after `notify`,
   so such a callback would deadlock; the `*Locked` helpers therefore collect evictions into a slice
   under the lock, and the public method unlocks before calling out. The cost is that a panic
   between `Lock` and `Unlock` would leave the mutex locked for good, so the locked region must stay
   short and predictable. Rule: never call user-supplied code while holding a lock; collect under
   the lock, call after releasing it.

   </details>
