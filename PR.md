# dispatch: cap concurrent deliveries per target host

One slow endpoint can take over the whole pool today: with `WORKERS=8`, eight deliveries to a host
that hangs until `ATTEMPT_TIMEOUT` stall every other customer's webhooks.

This adds `MAX_PER_HOST` (default 2, `0` disables the limit). Before each attempt a delivery waits
for a free slot for its target host, so a slow or failing host can hold at most `MAX_PER_HOST`
workers and deliveries to healthy hosts keep flowing. The limiter is a map of per-host semaphores
keyed by the URL's host; a delivery that is waiting for a slot still stops when the pool's context
is canceled, so the drain timeout keeps working.

Tests: `TestPoolLimitsConcurrencyPerHost` sends six deliveries to one slow target and checks that
at most `MaxPerHost` requests are in flight at once. The shared test config now sets a per-host
limit, so every existing pool test runs through the limiter.
