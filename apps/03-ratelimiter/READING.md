# 03 · ratelimiter

A small JSON API that serves Go proverbs and rate limits each client by IP address. The limiting
comes from the importable `ratelimit` package: a token bucket or a sliding window counter per
client, kept in memory and evicted when idle, with the decision reported in `X-RateLimit-*`
headers and a 429 with `Retry-After` once a client is over its limit.

## Run it

```sh
BURST=3 LIMIT=1 PERIOD=5s go run ./apps/03-ratelimiter/cmd/ratelimiter

# in another terminal
for i in 1 2 3 4; do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/api/proverbs/1; done; echo
curl -i localhost:8080/api/proverbs/1
```

Three requests spend the burst and the fourth gets 429; `curl -i` shows the rate limit headers and
`Retry-After`. `ALGORITHM=sliding_window` switches the algorithm, and `/healthz` is never limited.

## Questions

Answer from the code first, then open the answer.

1. You press Ctrl-C, the server starts draining, and you press Ctrl-C again. What happens, and what
   would change if `run` called `stop()` as soon as `ctx` was done?

   <details><summary>Answer</summary>

   `signal.NotifyContext` registers the signals with `signal.Notify` and keeps them registered until
   `stop` is called. The first SIGINT cancels `ctx`; later ones go to the context's internal
   channel, which nobody reads any more, so they are ignored and the process keeps draining until
   `srv.Shutdown` returns or `SHUTDOWN_TIMEOUT` runs out. Calling `stop()` right after
   `<-ctx.Done()` unregisters the signals and restores the default action, so a second Ctrl-C kills
   the process at once. `context.Cause(ctx)` reports which signal it was. Rule: the `stop` from
   `signal.NotifyContext` decides whether a repeated signal is ignored or fatal; call it as soon as
   shutdown starts if a second Ctrl-C should force the exit.

   </details>

2. SIGTERM arrives while a request is being served. Why does `run` build the shutdown deadline on
   `context.WithoutCancel(ctx)`, and what would `context.WithTimeout(ctx, cfg.ShutdownTimeout)` do?

   <details><summary>Answer</summary>

   When the `select` returns, `ctx` is already canceled: that is what woke it. A context derived
   from a canceled parent is born canceled, so `srv.Shutdown` would close the listener, see the
   request still active, and return `context.Canceled` at once; `run` would return an error and
   `main` would exit with status 1 in the middle of the request. `WithoutCancel` keeps the parent's
   values but drops its cancellation and deadline, so the new timeout alone bounds the drain and
   `Shutdown` waits for requests in flight. `context.Background()` would also work, but it loses the
   values and the `contextcheck` linter rejects it. Rule: cleanup that starts because a context was
   canceled needs a context detached from it, with its own deadline.

   </details>

3. When `NewTokenBucket` fails, `newLimiter` returns its result unchanged. Is the `limiter` that
   `run` receives equal to `nil`? Why is `run` still correct, and why does `require.Nil(t, l)` in
   `TestInvalidConfig` pass either way?

   <details><summary>Answer</summary>

   No. `return ratelimit.NewTokenBucket(...)` converts the `*TokenBucket` result into the `limiter`
   interface, and an interface value is a (type, value) pair: here (`*TokenBucket`, nil), which is
   not `nil` because the type half is set. `run` is still correct because it checks `err` first and
   never touches the limiter on error. testify's `Nil` inspects the dynamic value with reflection
   and accepts a typed nil pointer, while `l == nil` is false. The same conversion is used on
   purpose in `var _ Limiter = (*TokenBucket)(nil)`, a compile-time check that costs nothing at run
   time. Rule: branch on `err`, never on an interface result being `nil`, and return a literal `nil`
   when the interface itself must be nil.

   </details>

4. `/healthz` is never throttled, yet `DELETE /api/proverbs/4` spends a token before it gets 405.
   Why?

   <details><summary>Answer</summary>

   `server.New` builds two muxes. The outer one routes `GET /healthz` straight to its handler and
   hands everything under `/api/` to `limit(limited)`, the rate limit middleware wrapping the inner
   mux. A middleware is an `http.Handler` that runs before the handler it wraps, so it sees every
   request the outer mux sends into that subtree, including those the inner mux then rejects with
   404 or 405. Probes never eat into a client's budget, while malformed API calls do, which is what
   you want against a client hammering bad URLs. Rule: where a middleware sits in the mux tree
   decides which requests it sees; routing and method checks inside the wrapped handler run after
   it.

   </details>

5. `curl -i` prints `X-Ratelimit-Limit`, not `X-RateLimit-Limit` as the middleware writes it. Why,
   and which lookup in a test would find nothing?

   <details><summary>Answer</summary>

   `http.Header` is a `map[string][]string`, and its `Set`, `Add`, `Get` and `Values` methods pass
   the name through `textproto.CanonicalMIMEHeaderKey`, which upper-cases the first letter and every
   letter after a hyphen and lower-cases the rest. The map therefore holds `X-Ratelimit-Limit`:
   `rec.Header().Get("X-RateLimit-Limit")` finds it, while indexing the map directly with
   `rec.Header()["X-RateLimit-Limit"]` returns nil. Header names are case-insensitive on the wire,
   and HTTP/2 sends them in lower case anyway, so clients do not care. Rule: go through the
   `http.Header` methods, and index the map only with canonical names.

   </details>

6. Behind a load balancer at 10.0.0.1, with `TRUSTED_PROXIES=10.0.0.0/8`, a client sends its own
   `X-Forwarded-For: 1.2.3.4` on every request. Which key does `ForwardedFor` choose, and what would
   go wrong if it read the header from the left, or trusted it from any peer?

   <details><summary>Answer</summary>

   Each proxy appends the address it received the request from, so the app sees
   `1.2.3.4, <client address>`. `ForwardedFor` walks the hops from the right with `slices.Backward`,
   a range-over-func iterator whose loop body runs as a callback (a `return` inside it just stops
   the iteration), skips trusted proxies, and keys on the first untrusted address: the one the load
   balancer saw. Reading from the left would pick `1.2.3.4`, which the client chooses and can change
   on every request to get a fresh bucket. Trusting the header from an untrusted peer opens the same
   hole without any proxy involved. Rule: only the entries your own proxies appended are facts; key
   on the rightmost address that is not one of your proxies.

   </details>

7. Two requests for the same key arrive microseconds apart, on either side of a window boundary.
   Why does `store.do` call `s.clock.Now()` only after taking the mutex? What would the sliding
   window do if the earlier timestamp were applied second?

   <details><summary>Answer</summary>

   Reading the clock inside the critical section makes the order of timestamps match the order in
   which they are applied to the state. If each goroutine read the clock first and then raced for
   the lock, the one holding the earlier time could apply it second. `advance` would then compute
   `start.Sub(w.start) == -period`, fall into the `default` case and zero both counters: with a
   limit of 2, a client that had just been rejected would get two more requests through in the same
   trailing second. The token bucket would only report `Retry-After` and reset times inflated by the
   gap. Rule: when a state transition depends on "now", read the clock under the lock that orders
   the transitions.

   </details>

8. `main` defers `limiter.Close()`. In `store.close`, what does `close(s.stop)` do to `evictLoop`,
   why does `close` then wait on `s.done`, and what does a second, concurrent `Close` see?

   <details><summary>Answer</summary>

   A receive from a closed channel succeeds immediately, so closing `stop` is a broadcast that
   carries no value and wakes every receiver; `evictLoop`'s `case <-s.stop` fires and the goroutine
   returns. Its deferred calls run last in, first out: `t.Stop()`, then `close(s.done)`, so
   `<-s.done` in `close` returns only once the loop can no longer touch the map, and only then is
   the map cleared. `sync.Once` guards `close(s.stop)`, because closing a closed channel panics; a
   second caller blocks inside `Once.Do` until the first one finishes, then returns. In Python
   terms: a `threading.Event` to stop and `Thread.join()` to wait. Rule: a stoppable goroutine needs
   a stop signal and a done signal, and `Close` should be safe to call twice.

   </details>
