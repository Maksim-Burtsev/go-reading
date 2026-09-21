# 05-lru-cache

A generic, thread-safe LRU cache with TTL, used by a read-through HTTP proxy that collapses concurrent misses with singleflight.

## Run

```sh
go run ./apps/05-lru-cache/cmd/...
```

Then run `curl -i localhost:8080/golang.org/x/sync/@v/list` twice: the first response has `X-Cache: MISS`, the second `X-Cache: HIT`. The upstream defaults to `https://proxy.golang.org`. `go test -bench . ./apps/05-lru-cache/lru` runs the benchmarks.

## Where to start

1. `cmd/lru-cache/main.go:run` builds the config, the cache options, the proxy and the HTTP server, and ties their lifetimes together with an errgroup.
2. `lru/lru.go:Cache` holds a doubly linked list for recency, a map for lookup, and the counters, all behind one mutex.
3. `lru/lru.go:Cache.Set` goes through `setLocked` and `evictLocked`: insertion, capacity eviction, and the eviction records that `notify` hands to `OnEvict`.
4. `internal/proxy/proxy.go:Proxy.handleGet` is the request path: the cache key, the hit, and the fallback to `load` on a miss.
5. `internal/proxy/proxy.go:Proxy.load` collapses concurrent misses for one key into a single upstream fetch that outlives the requests waiting on it.

## Data flow

1. `GET /some/path?q=1` reaches the mux built in `Proxy.Handler`; `requestKey` turns the escaped path and query into the cache key.
2. `Cache.Get` finds the list element, checks its deadline, moves it to the front and counts a hit; the stored response is written with `X-Cache: HIT`.
3. On a miss, `load` joins or starts a singleflight flight for the key and waits on either the result or the client's context.
4. The flight re-checks the cache with `Peek`, then `fetch` calls the upstream with a context detached from the client, reads at most `MAX_BODY_BYTES`, and keeps three headers.
5. A `200` response is stored with `Cache.Set`; if that pushes the cache over capacity, the back element is removed and `OnEvict` runs after the lock is released, logging at debug level.
6. Every waiter receives the same `*Response`; upstream failures become `502`, timeouts `504`.
7. Alongside requests, `sweep` calls `DeleteExpired` every `JANITOR_INTERVAL`, `PURGE /path` calls `Remove`, and `GET /_cache/stats` reads `Stats`.

## Go specifics here

1. `cmd/lru-cache/main.go:97` — `lru.WithTTL[string, *proxy.Response]` next to a bare `lru.WithOnEvict(...)`.
   <details><summary>Explanation</summary>

   Go infers type parameters only from the arguments of a call, never from where the result goes. `WithTTL` takes a `time.Duration`, which says nothing about `K` or `V`, so the caller has to spell them out. `WithOnEvict` takes a `func(string, *proxy.Response, lru.EvictReason)`, from which both are inferred. `lru.New` then infers its own `K` and `V` from the options it is given. Python's `TypeVar`s are erased at runtime; Go instantiates a real type, so it must know `K` and `V` at compile time.
   </details>

2. `lru/lru.go:245` — `el.Value.(*entry[K, V])`.
   <details><summary>Explanation</summary>

   `container/list` predates generics, so `list.Element.Value` is `any`. `x.(T)` is a type assertion: it checks at runtime that the interface holds exactly a `*entry[K, V]`, and panics if it does not. The two-value form `v, ok := x.(T)` would not panic. The single-value form is safe here because the cache is the only code that puts values into the list. The rest of the package is fully typed, so this is the one spot where static typing gives way.
   </details>

3. `lru/lru.go:147` — `c.mu.Unlock()` without `defer`, followed by `c.notify(evicted)`.
   <details><summary>Explanation</summary>

   `sync.Mutex` is not reentrant, unlike `threading.RLock`: if the goroutine holding it calls `Lock` again, it deadlocks. The `OnEvict` callback is user code and may call back into the cache, so it must run after the unlock. `defer c.mu.Unlock()` would only run when `Get` returns, after `notify`. So the `*Locked` helpers collect the evictions into a slice while the lock is held, and the public method unlocks explicitly before dispatching them.
   </details>

4. `internal/proxy/proxy.go:134` — `context.WithoutCancel(ctx)` inside the function passed to `DoChan`.
   <details><summary>Explanation</summary>

   The flight is shared by every request waiting on the key, but `ctx` belongs to whichever request started it. If the flight used that context, one client disconnecting would cancel the fetch for all of them. `WithoutCancel` keeps the context's values but drops its cancellation and deadline, so `http.Client.Timeout` is what bounds the fetch. Each waiter still stops waiting on its own `ctx.Done()` in the `select` below, and the result is cached even if nobody is left waiting for it.
   </details>

5. `cmd/lru-cache/main.go:155` — `type expirer interface { DeleteExpired() int }`.
   <details><summary>Explanation</summary>

   Go interfaces are satisfied implicitly: `*lru.Cache[string, *proxy.Response]` never names `expirer`, yet it can be passed to `sweep` because it has the method. The interface is declared by the consumer, sized to exactly what `sweep` calls. It also keeps `sweep` non-generic: without it, `sweep` would need its own `[K, V]` type parameters just to accept the cache. This is structural typing, the same idea as a `typing.Protocol`, except the compiler checks it at the call site.
   </details>

## Questions

1. `Peek` neither counts a hit nor refreshes recency. Where does the proxy call it, and what would go wrong if it called `Get` there instead?
   <details><summary>Answer</summary>

   Inside the singleflight function in `Proxy.load`. It closes a race: a request can miss in `handleGet` just as an earlier flight for the same key stores its result and finishes, so the new request starts a second flight. The `Peek` returns the fresh entry instead of fetching again. With `Get`, the same logical request would be counted twice, once as a miss in `handleGet` and once more as a hit or miss, which skews `hit_ratio`, and it would also bump recency for a key nobody has been served yet.
   </details>

2. A client requests an uncached URL and disconnects after 100 ms, while the upstream takes 2 s. What happens to the fetch, to other clients waiting on the same URL, and to the cache?
   <details><summary>Answer</summary>

   That client's `load` returns from `select` through `ctx.Done()`. `writeError` sees `context.Canceled`, logs "client left before upstream answered" and writes nothing. The fetch keeps going because it runs under `context.WithoutCancel`, bounded only by `UPSTREAM_TIMEOUT`. Other waiters receive the response from the same flight, and a `200` is stored in the cache, so the next request is a hit. `TestFetchOutlivesCanceledRequest` covers exactly this.
   </details>

3. With `CACHE_TTL=5m`, `Len()` can report more entries than are actually servable. Why, and what bounds the memory held by entries nobody will ever read again?
   <details><summary>Answer</summary>

   Expiry is lazy: an expired entry stays in the list until a `Get` touches it, until it reaches the back of the list and is pushed out by a `Set`, or until `DeleteExpired` runs. The service starts `sweep`, which calls `DeleteExpired` every `JANITOR_INTERVAL`. In the worst case, memory is bounded by `CACHE_SIZE × MAX_BODY_BYTES` plus headers, because capacity counts every entry, expired or not.
   </details>

4. The oldest entry has already expired when a `Set` pushes the cache over capacity. Which counter goes up, which reason does `OnEvict` receive, and why is that the right choice?
   <details><summary>Answer</summary>

   `setLocked` checks the back element with `expiredLocked`, so `Expirations` goes up and the callback gets `ReasonExpired`. `Evictions` is meant to measure capacity pressure. An entry that was already dead would have been dropped anyway, and counting it as a capacity eviction would suggest the cache is too small when it is not.
   </details>

5. On `SIGTERM`, which goroutines are running, and what makes each one return? What changes if `Serve` fails on its own first?
   <details><summary>Answer</summary>

   `signal.NotifyContext` cancels `ctx`, which cancels `gctx`. The errgroup runs up to three goroutines. The shutdown goroutine wakes on `gctx.Done()` and calls `srv.Shutdown` with a timeout derived from `context.WithoutCancel`, so the timeout is not cancelled immediately. `Serve` then returns `http.ErrServerClosed`, which is treated as success. `sweep` returns on `gctx.Done()`. If `Serve` fails first, it returns an error, and the errgroup cancels `gctx`, so shutdown and sweep exit the same way. `g.Wait()` returns that first error and `main` exits with status 1.
   </details>
