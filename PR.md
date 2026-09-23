# 03-ratelimiter: delay over-limit requests instead of rejecting them

Browsers and SDKs often fire a handful of requests at once, and with a small burst the tail of that
fan-out gets 429 even though a token frees up a few milliseconds later. This adds an opt-in wait to
the middleware, much like nginx's `limit_req ... delay`: a request over the limit waits for the
limiter's `RetryAfter` and tries again, as long as the total wait stays within `MaxWait`.
`MaxWaiters` caps how many requests can be parked at once, so a flood cannot pile up goroutines.

- `ratelimit.Middleware` takes options, `WithMaxWait` and `WithMaxWaiters`. Without them nothing
  changes.
- The demo server reads `MAX_WAIT` (default `0s`, which keeps the feature off) and `MAX_WAITERS`
  (default 100).

Tests: `TestMiddlewareDelaysRequestUntilAllowed`, `TestMiddlewareRejectsWhenWaitIsTooLong`,
`TestMiddlewareCapsWaiters`, `TestMiddlewareStopsWaitingWhenClientLeaves` and
`TestMiddlewareDelaysKeysIndependently`. The ones that wait run under `testing/synctest`, so they
take no real time. `TestLoad` covers the two new variables.
