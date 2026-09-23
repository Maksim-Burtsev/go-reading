# 004 · dispatch: cap concurrent deliveries per target host

4 defects, 2 decoys.

## 1. Host slots are released when the delivery ends, not after each attempt (high)

`apps/04-worker-pool/internal/dispatch/pool.go:103`, in `deliver`

What goes wrong: a deferred call runs when the surrounding function returns, not at the end of the
loop iteration. Each attempt therefore keeps the slot it took for its host, through the backoff
sleeps and into the following attempts. With the defaults (`MAX_ATTEMPTS=5`, `MAX_PER_HOST=2`), a
target that answers 503 twice leaves the delivery holding both of its host's slots. The third
attempt then blocks in `acquire` on a slot that only this goroutine could free, and waits until the
drain cancels `workCtx`. Every other delivery to that host queues behind it, each one also holding
a worker. Once `WORKERS` deliveries are stuck this way the pool stops, the queue fills, and every
`POST /deliveries` gets 503. Reproduced with `MaxPerHost=2` and responses 503, 503, 200: the
delivery was still blocked when the test's 2 s deadline ended it, reported `canceled` after 2
attempts.

The tell: a `defer` inside a `for` body. Also, `testConfig` now needs a per-host limit of 4, above
both `Workers` and `MaxAttempts`, which is exactly the value that keeps the three-attempt tests
from hanging.

Fix:

```go
		var release func()
		release, err = p.hosts.acquire(ctx, host)
		if err != nil {
			return Result{TaskID: t.ID, Status: StatusCanceled, Attempts: attempt, Err: err}
		}
		err = p.attempt(ctx, t)
		release()
```

Rule: `defer` belongs to the function, not the block; to release something per iteration, call the
release yourself or move the loop body into its own function.

Found if: names the `defer release()` inside the retry loop and says the slots stay taken until the
delivery finishes (so a delivery can block on its own slots, or slots are held across backoff).

## 2. A delivery waiting for its host still occupies a worker (medium)

`apps/04-worker-pool/internal/dispatch/pool.go:99`, in `deliver`, which runs inside the `g.Go`
goroutine started at `pool.go:68`

What goes wrong: `Run` bounds the goroutines with `g.SetLimit(p.cfg.Workers)`, and `deliver` runs
inside one of them. A delivery blocked in `acquire` keeps its errgroup slot while it waits. The PR
says a slow host "can hold at most `MAX_PER_HOST` workers", but every task for that host that
reaches a worker holds one. Take eight tasks for a slow host S queued ahead of one task for a
healthy host H, with `WORKERS=4` and `MAX_PER_HOST=1`: one S delivery sends, three wait for S's
slot, all four workers are taken, and H waits behind the whole S backlog. Measured with S answering
in 200 ms, H was delivered after 1.0 s with the limit and after 0.4 s without it: the change makes
the healthy host slower, the opposite of its purpose.

The tell: the wait is added to code that already runs under the worker limit, and nothing changes
the order in which `Run` takes tasks off the queue, so no healthy task can overtake a stuck one.

Fix: a worker must never wait for a host. That needs a structural change, not a one-liner. For
example, `Run` can route each task to a bounded queue per host, drained by at most `MaxPerHost`
goroutines that take a global worker slot only when they have a task to send. A task whose host
queue is full fails at once instead of blocking the dispatcher:

```go
q, ok := byHost[host]
if !ok {
	q = make(chan Task, hostQueueSize)
	byHost[host] = q
	for range p.cfg.MaxPerHost {
		drainers.Go(func() {
			for t := range q {
				workers <- struct{}{}
				p.results <- p.deliver(ctx, t)
				<-workers
			}
		})
	}
}
select {
case q <- task:
default:
	p.results <- Result{TaskID: task.ID, Status: StatusFailed, Err: ErrHostBusy}
}
```

Rule: a limit acquired inside a slot of another limit still consumes the outer slot while it waits;
nested semaphores give isolation only if the inner wait happens outside the outer slot.

Found if: says that waiting for the host slot happens inside the errgroup-limited goroutine, so a
slow host can still take every worker (or that the PR does not deliver the isolation it claims).

## 3. `MAX_PER_HOST=0` stops every delivery instead of disabling the limit (medium)

`apps/04-worker-pool/internal/dispatch/hosts.go:27`, in `acquire` (the promise is at `pool.go:24`
and in `PR.md`; `main.go:52` lets 0 through)

What goes wrong: the doc comment and the PR description say zero means no limit, and validation
accepts 0, but no code checks for it. `make(chan struct{}, 0)` creates an unbuffered channel. A send
on it completes only when a receiver is waiting, and the only receiver is a `release` that nobody
can call without first holding a slot. Every delivery blocks in `acquire` until the drain cancels
it, the queue fills, and every `POST` gets 503. That happens exactly when an operator sets 0 to
switch the feature off, for example during an incident.

The tell: the zero value is given a meaning in two places, and no line of the change looks at
`l.limit == 0`.

Fix:

```go
func (l *hostLimiter) acquire(ctx context.Context, host string) (func(), error) {
	if l.limit == 0 {
		return func() {}, nil
	}
	l.mu.Lock()
```

Rule: when zero is documented as "unlimited", handle it explicitly; a channel with capacity zero is
not "no limit", it is a rendezvous.

Found if: names the zero setting (unbuffered semaphore) and says it blocks all deliveries instead of
disabling the limit.

## 4. The per-host test would pass without the limiter (low)

`apps/04-worker-pool/internal/dispatch/pool_test.go:308`, in `TestPoolLimitsConcurrencyPerHost`

What goes wrong: the test keeps `Workers: 2` from `testConfig` (`pool_test.go:30`) and sets
`MaxPerHost = 2`, so the errgroup limit alone never lets more than two requests run. The assertion
`peak <= MaxPerHost` holds whether the limiter works or not. With the limiter's capacity raised to
1000 the test still passes, and it cannot catch any of the defects above.

The tell: the asserted bound equals `Workers`. A per-host limit is only observable when more
workers than host slots are available, and the isolation claim needs a second host.

Fix:

```go
	cfg := testConfig()
	cfg.Workers = 6
	cfg.MaxPerHost = 2
```

Rule: test a limit in a setup where something else would exceed it, or the test proves nothing.

Found if: says the test cannot fail because `Workers` equals `MaxPerHost`, or otherwise that the
errgroup limit alone satisfies it.

## Not defects

### Returning the address of a local

`apps/04-worker-pool/internal/dispatch/hosts.go:17`: `newHostLimiter` returns `&l` for a local
variable. In C that pointer would dangle once the function returns; in Go, escape analysis moves
`l` to the heap and the pointer stays valid. Constructors do this all the time.

### Unlocking before the wait instead of deferring the unlock

`apps/04-worker-pool/internal/dispatch/hosts.go:30`: `acquire` calls `l.mu.Unlock()` explicitly
before the blocking `select`, where "always `defer` the unlock" would suggest otherwise. Deferring
it would hold the map lock while waiting for a slot and make every host wait behind one busy host.
The lock only guards the map lookup, so releasing it right after is deliberate and correct.

## Also acceptable

- The `slots` map never shrinks: one channel per distinct host ever seen, unbounded when clients can
  submit arbitrary hostnames.
- The key is `u.Host`, which includes the port and is case-sensitive, so `Example.com`,
  `example.com` and `example.com:443` get separate limits.
- `NewPool` does not reject a negative `MaxPerHost`; `make` panics on the first delivery. Only
  `main` validates it, as it does for `Workers`.
- The default of 2 cuts throughput for a deployment that mostly delivers to one host: 8 workers,
  2 requests in flight.
