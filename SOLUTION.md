# 003 · 03-ratelimiter: delay over-limit requests instead of rejecting them

4 defects, 2 decoys.

## 1. The decision `wait` returns is shadowed and thrown away (high)

`apps/03-ratelimiter/ratelimit/http.go:87`, in `ServeHTTP`

What goes wrong: `d, err := h.wait(ctx, key, d)` declares a new `d` and `err` that live only inside
the `if` block. After the block, `ServeHTTP` still sees the first decision, a rejection, and a nil
error. With `LIMIT=10`, `PERIOD=1s`, `BURST=1` and `MAX_WAIT=500ms`, a second request arriving
together with the first waits 100 ms, the `Allow` inside `wait` takes the refilled token, and the
client still gets 429 with `Retry-After: 1`. The next handler never runs for a delayed request, and
the token it took is gone, so the following request is rejected too. A wait that ends because the
client left loses its error as well and falls through to writing a 429. The feature adds latency
and burns tokens, but never lets through a request it delayed.

The tell: `:=` on the left of a call whose results are meant to replace the `d` and `err` already in
scope; inside the block, the new `d` only feeds a debug log.

Fix:

```go
		d, err = h.wait(ctx, key, d)
```

Rule: `:=` in an inner block declares new variables even when the names exist outside it; to update
the outer ones, assign with `=`.

Found if: the verdict points at the `h.wait` call in `ServeHTTP` and says its result never reaches
the code after the block, whether it calls that shadowing or says a delayed request still gets 429.

## 2. The waiter cap is a check-then-act race (medium)

`apps/03-ratelimiter/ratelimit/http.go:117`, in `wait`

What goes wrong: `h.waiting.Load() >= h.maxWaiters` and the `h.waiting.Add(1)` three lines below
are two separate atomic operations. Each is atomic, the pair is not: requests that arrive together
all read the counter before any of them increments it, and all of them start waiting. A retry storm
of 1,000 simultaneous over-limit requests with `MAX_WAITERS=100` parks far more than 100
goroutines, which is what the cap exists to prevent. In a burst of 32 requests against a cap of 1,
two waited at once.

The tell: a limit enforced by a `Load` followed by a separate `Add`. The race detector cannot see
it, since every access is atomic, and `TestMiddlewareCapsWaiters` sends its second request only
after `synctest.Wait()` has parked the first, so the window is never exercised.

Fix:

```go
	if n := h.waiting.Add(1); h.maxWaiters > 0 && n > h.maxWaiters {
		h.waiting.Add(-1)
		return d, nil
	}
	defer h.waiting.Add(-1)
```

Rule: an atomic counter enforces a limit only when the operation that claims a slot is also the one
that checks it: increment and compare the result, or use `CompareAndSwap`, never `Load` then `Add`.

Found if: the verdict names the `Load` and `Add` pair in `wait` and says concurrent requests can all
pass the check.

## 3. `MaxWait` bounds each retry, not the total wait (medium)

`apps/03-ratelimiter/ratelimit/http.go:123`, in `wait`

What goes wrong: `WithMaxWait` promises a wait "for up to d in total", and the comment on `wait`
says the same, but the loop condition compares each single `RetryAfter` with `maxWait` and nothing
accumulates. With `LIMIT=5`, `PERIOD=1s`, `BURST=10` and `MAX_WAIT=500ms`, an office behind one NAT
address loads a page that fires 30 requests: 10 pass and 20 wait, every wake-up lets one of them
through, and the rest get a fresh 200 ms `RetryAfter`, which passes the check again. The last one
is admitted about 4 s later, eight times `MAX_WAIT`. A key that never frees up holds its request
until the client disconnects; `WriteTimeout` does not cancel the request context, so the handler
keeps looping after its response can no longer be delivered.

The tell: a loop that promises a total bound has no deadline and no accumulated duration.
`TestMiddlewareRejectsWhenWaitIsTooLong` only tries a single `RetryAfter` that is already over the
limit.

Fix:

```go
	deadline := time.Now().Add(h.maxWait)
	for !d.Allowed && !time.Now().Add(d.RetryAfter).After(deadline) {
```

Rule: a time budget for a whole operation is a deadline computed once, before the loop, not a check
on each step.

Found if: the verdict says the total wait can exceed `MaxWait` because the loop compares only one
`RetryAfter` at a time.

## 4. The delay test never checks what the client gets (low)

`apps/03-ratelimiter/ratelimit/http_test.go:252`, in `TestMiddlewareDelaysRequestUntilAllowed`

What goes wrong: the test is named after an outcome, a delayed request that is let through, but it
asserts only that `Allow` ran twice and that 50 ms of fake time passed. The recorder is created
inline and never read, so the test passes while every delayed request gets 429 (defect 1).
`TestMiddlewareDelaysKeysIndependently` has the same gap.

The tell: `httptest.NewRecorder()` passed inline and discarded in a test whose name promises a
response.

Fix:

```go
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

		require.Equal(t, http.StatusOK, rec.Code)
```

Rule: a test asserts the outcome its name promises; checking only that the mechanism ran lets a
broken outcome through.

Found if: the verdict says the delay tests never check the status of the delayed request.

## Not defects

### `time.After` inside the retry loop

`apps/03-ratelimiter/ratelimit/http.go:127`: before Go 1.23, a `time.After` whose `select` lost to
`ctx.Done()` kept its timer alive until it fired, so a loop like this one piled up timers. Since Go
1.23, and this module is on 1.27, an unreferenced timer is collected even if it never fires, and
timer channels are unbuffered, so there is no leak and no stale value.

### The test goroutines use the loop variable `i`

`apps/03-ratelimiter/ratelimit/http_test.go:343`: in Python a closure created in a loop sees the
variable's last value. Since Go 1.22 each iteration of a `for` loop has its own `i`, so each
goroutine builds its own key; the expected map, `client-0` to `client-2` with two calls each,
depends on it. `wg.Go` (Go 1.25) does the `Add` and the `Done`.

## Also acceptable

- `MAX_WAIT` is not checked against the server's 10 s `WriteTimeout`: a longer wait ends in a
  response the client never receives.
- Negative `MAX_WAIT` and `MAX_WAITERS` are accepted silently; a negative `MAX_WAITERS` removes the
  cap.
- The waiter cap belongs to each handler the middleware wraps, not to the middleware: wrapping two
  handlers doubles it.
- `MAX_WAITERS=0` also means no cap, the opposite of what an operator setting 0 expects.
- One flooding client can hold every waiter slot, so other clients over their limit get an
  immediate 429; waiters on one key all wake together and retry under the store's single mutex.
- A `Limiter` that rejects with `RetryAfter` 0 makes the wait loop spin; the interface does not
  rule it out, although both built-in limiters return a positive value.
- Parked requests are not released on shutdown, so they hold `srv.Shutdown` for up to `MAX_WAIT`
  each round.
