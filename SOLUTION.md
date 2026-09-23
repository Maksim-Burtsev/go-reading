# 005 · 05-lru-cache: persist the cache across restarts

5 defects, 2 decoys.

## 1. `encoding/json` drops the response, and restored hits crash (high)

`apps/05-lru-cache/internal/proxy/proxy.go:75`, in `snapshotEntry`

What goes wrong: `Response` has only unexported fields (`status`, `header`, `body`), and
`encoding/json` silently skips unexported fields, so every entry is written as
`{"key":"/ok","response":{}}` and restored as a zero `Response`. After a restart, a request for a
restored key is a cache hit, and `serve` calls `w.WriteHeader(0)`, which panics with "invalid
WriteHeader code 0". net/http recovers the panic, logs "http: panic serving" and drops the
connection, so the client gets an empty reply. Every warmed URL, the most recently used ones by
construction, fails this way until its TTL runs out or it is purged: with a snapshot, a restart is
worse than a cold start.

The tell: a JSON DTO that holds a type whose fields are all lowercase, with no `MarshalJSON`.
staticcheck's check for marshaling a struct without exported fields (SA9005) looks only at the value
passed to `Encode`, and `snapshotEntry` has exported fields, so CI stays green.

Fix:

```go
type snapshotEntry struct {
	Key    string      `json:"key"`
	Status int         `json:"status"`
	Header http.Header `json:"header"`
	Body   []byte      `json:"body"`
}
```

with `snapshotEntry{Key: key, Status: resp.status, Header: resp.header, Body: resp.body}` in
`SaveSnapshot` and `&Response{status: e.Status, header: e.Header, body: e.Body}` in `LoadSnapshot`.

Rule: `encoding/json` sees only exported fields and says nothing about the rest, so round-trip every
type you serialize and give unexported state an explicit DTO or a `MarshalJSON`.

Found if: the verdict says the snapshot loses the responses because `Response`'s fields are
unexported, or that restored entries come back empty.

## 2. The loop body over `All` runs under the cache lock (high)

`apps/05-lru-cache/lru/lru.go:229`, in `All`

What goes wrong: `All` locks `c.mu` and, through the `defer`, holds it for the whole iteration; with
a range-over-func iterator the caller's loop body is `yield`, so it runs inside that critical
section. The loop body in `SaveSnapshot` JSON-encodes each entry and writes it straight to the file,
one write per entry, so every `SNAPSHOT_INTERVAL` all `Get` and `Set` calls, and with them all
requests, wait until the snapshot is on disk. Once defect 1 is fixed that is up to
`CACHE_SIZE x MAX_BODY_BYTES` of base64, about 1.4 GB with the defaults: seconds of stall every
minute. Any cache call inside such a loop deadlocks, because `sync.Mutex` is not reentrant: a
progress log calling `p.cache.Len()` would hang the snapshot goroutine for good, with the lock held.
In a test, a concurrent `Get` took 201 ms against a loop body of 200 ms, and `Len()` inside the loop
never returned.

The tell: `c.mu.Lock()` with a deferred unlock around the calls to `yield`, in a package whose other
methods collect evictions under the lock and run `OnEvict` only after releasing it.

Fix (copy under the lock, yield after releasing it; walking the list also fixes defect 3):

```go
	return func(yield func(K, V) bool) {
		c.mu.Lock()
		now := c.now()
		live := make([]entry[K, V], 0, c.order.Len())
		for el := c.order.Back(); el != nil; el = el.Prev() {
			if e := el.Value.(*entry[K, V]); c.ttl <= 0 || !e.expiredAt(now) {
				live = append(live, *e)
			}
		}
		c.mu.Unlock()
		for _, e := range live {
			if !yield(e.key, e.value) {
				return
			}
		}
	}
```

Rule: the body of a range-over-func loop runs inside the iterator, so an iterator must not hold a
lock while it calls `yield`.

Found if: the verdict says the lock taken in `All` is held while the caller's loop body, the encoding
and the file writes, runs, or that calling the cache from inside the loop deadlocks.

## 3. `All` walks the map, so its order is random (medium)

`apps/05-lru-cache/lru/lru.go:232`, in `All`

What goes wrong: recency lives in the list, `c.order`; `c.items` is a map, and Go randomizes map
iteration order on purpose. `All` promises entries "from the least to the most recently used", and
`LoadSnapshot` replays them in file order, so after a restart the cache's recency is a random
permutation. With a full cache, the first misses after the restart evict whatever landed at the
back, often hot entries, which then go to the upstream again: the cold start the PR set out to
avoid. The same six-entry cache came out in 6 different orders over 20 calls.

The tell: an order promise in the doc comment right above `range c.items`, and a new test that
compares the keys with `require.ElementsMatch`, which ignores order, for an API documented as
ordered.

Fix:

```go
		for el := c.order.Back(); el != nil; el = el.Prev() {
			e := el.Value.(*entry[K, V])
```

and `yield(e.key, e.value)` in the body.

Rule: never let map iteration order reach output that promises an order; walk a structure that has
one, or sort.

Found if: the verdict says `All` ranges over the map, so the order is not least to most recently
used.

## 4. A partial snapshot stops the proxy from starting (medium)

`apps/05-lru-cache/cmd/lru-cache/main.go:119`, in `run`, and
`apps/05-lru-cache/cmd/lru-cache/main.go:196`, in `writeSnapshot`

What goes wrong: `writeSnapshot` truncates the live file with `os.Create` and writes into it, every
minute and at shutdown. A kill in the middle of a write, by the OOM killer or by SIGKILL at the end
of the orchestrator's grace period, leaves a partial file. On the next start `LoadSnapshot` fails,
`run` returns "restore cache: decode snapshot entry 1: unexpected EOF", and the process exits with
status 1, on every restart, until someone deletes the file. A warm-up that exists to save upstream
requests takes the whole proxy down.

The tell: a best-effort optimization whose failure `run` returns as fatal, and `os.Create` on the
final path. `TestLoadSnapshotRejectsCorruptInput` checks that `LoadSnapshot` reports a corrupt
file, which makes the fatal return look deliberate, but nothing starts the proxy with one.

Fix:

```go
		n, err := readSnapshot(p, cfg.SnapshotPath)
		if err != nil {
			logger.WarnContext(ctx, "cache not restored, starting cold", "path", cfg.SnapshotPath, "error", err)
		} else {
			logger.InfoContext(ctx, "cache restored", "entries", n, "path", cfg.SnapshotPath)
		}
```

and write the snapshot to `os.CreateTemp` in the same directory, `Sync`, `Close`, then `os.Rename`
it over the old one, so a crash never leaves a partial file behind.

Rule: when an optimization fails, the service degrades instead of stopping, and a file that is
rewritten while others may read it is replaced atomically, through a temp file and a rename.

Found if: the verdict says a corrupt or partial snapshot makes startup fail, or that the snapshot is
written in place so a crash corrupts it; either counts.

## 5. The round-trip test never compares what comes back (low)

`apps/05-lru-cache/internal/proxy/proxy_test.go:417`, in `TestSnapshotRoundTrip`

What goes wrong: the test checks the count `LoadSnapshot` returns, `Len()`, and that `Peek` finds
each key, but never the restored status, headers or body, and it never serves a restored key. It
passes although every restored response is empty (defect 1); serving `/ok` from the restored proxy
would have panicked inside the test.

The tell: a test named for a round trip that never compares the output with the input.

Fix:

```go
	for _, key := range []string{"/ok", "/ok?v=2"} {
		rec := serve(t, dst.Handler(), http.MethodGet, key)
		require.Equal(t, http.StatusOK, rec.Code, key)
		require.Equal(t, "HIT", rec.Header().Get("X-Cache"), key)
		require.Equal(t, "hello", rec.Body.String(), key)
	}
```

Rule: a serialization test compares what it decodes, or better its observable behavior, with the
original; presence is not equality.

Found if: the verdict says the round-trip test only checks that the keys exist.

## Not defects

### Returning from inside the range over `All`

`apps/05-lru-cache/internal/proxy/proxy.go:84`: a Python developer may expect an early `return` out
of the loop to skip the iterator's cleanup and leave `c.mu` locked. With range-over-func, leaving
the loop makes `yield` return false; `All` then returns, and its deferred `Unlock` runs, so the lock
is released (after a failed save, `Len()` still works). An iterator that kept calling `yield` after
it returned false would make the runtime panic.

### Storing `&e.Response` from the loop variable

`apps/05-lru-cache/internal/proxy/proxy.go:108`: before Go 1.22 the loop variable `e` was one
variable reused by every iteration, and every cached pointer would have pointed at the last entry.
Since Go 1.22 each iteration has its own `e`, so each `&e.Response` is distinct (checked with two
restored keys).

## Also acceptable

- Restored entries restart their TTL, because `Set` computes a fresh `expiresAt`, so a response can
  outlive `CACHE_TTL` across restarts.
- The snapshot file is created with mode 0666 minus the umask and holds cached upstream content;
  0600 is tighter.
- The final snapshot is skipped when `g.Wait()` returns an error, for example after a shutdown
  timeout.
- `SNAPSHOT_INTERVAL` is validated even when `SNAPSHOT_PATH` is empty.
- `LoadSnapshot` reads every entry into memory before storing any, even past `CACHE_SIZE`, and
  reports the entries it decoded rather than the ones the cache kept.
- The snapshot records neither a format version nor the upstream, so after a change of
  `UPSTREAM_URL` the old origin's content is served.
- Keys travel as JSON strings, so a key with invalid UTF-8 comes back with U+FFFD in it.
- `readSnapshot`, `writeSnapshot` and the `SNAPSHOT_*` settings have no tests.
