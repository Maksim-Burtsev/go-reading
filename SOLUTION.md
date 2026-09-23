# 011 · batcher: bound the buffer by bytes, not only by events

5 defects, 2 decoys.

## 1. A dropped batch never gives its bytes back (high)

`apps/11-clickhouse-sink/internal/batcher/batcher.go:226`, in `flush`

What goes wrong: `flush` returns early when the insert fails, and `b.bytes.Add(-size)` sits after that
return. During a ClickHouse outage every dropped batch leaves its payload counted, so once the dropped
payload adds up to `BUFFER_MAX_BYTES`, `Enqueue` answers 429 to every request although the buffer is
empty. The service never recovers without a restart.

The tell: the acquire (`b.bytes.Add(size)` in `Enqueue`) has one release, placed after an early
`return err`. Master's `flush` had no early return; the PR added one in the same hunk as the release.

Fix:

```go
	size := payloadBytes(batch)
	b.bytes.Add(-size)
	b.batchBytes.Observe(float64(size))
	if err != nil {
		b.dropped.Add(float64(len(batch)))
		b.logger.ErrorContext(ctx, "batch dropped", "events", len(batch), "error", err)
		return err
	}
	return nil
```

Rule: every acquire needs a release on every way out, errors included; release before the branch or in
a `defer`.

Found if: the verdict says that the bytes of a batch are not released when the insert fails (the drop
path in `flush`), so the budget leaks.

## 2. The byte budget is checked outside the lock (medium)

`apps/11-clickhouse-sink/internal/batcher/batcher.go:134`, in `Enqueue`

What goes wrong: `b.bytes.Load()+size > b.cfg.MaxBytes` runs before `b.mu.Lock()`, and
`b.bytes.Add(size)` runs later, inside the lock. Concurrent requests all read the same old value, all
pass, and all add: with 400 concurrent 100-byte requests against a 10 000-byte budget, 40 000 bytes get
buffered. The budget fails exactly under the concurrent load it exists for. The early check also runs
before the `closed` check, so during shutdown a request can get 429 instead of 503.

The tell: check and act on shared state in two different critical sections. The atomic makes each step
safe, not the pair. Master's event-count check two lines further down is correct because it sits under
the same lock as the sends.

Fix:

```go
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}
	if cap(b.events)-len(b.events) < len(events) || b.bytes.Load()+size > b.cfg.MaxBytes {
		return ErrBufferFull
	}
	b.bytes.Add(size)
```

Rule: a check and the update it guards belong in one critical section; atomics do not make a
check-then-act sequence atomic.

Found if: the verdict names the byte check before the lock (or check-then-act between `Load` and `Add`),
letting concurrent requests exceed the budget.

## 3. `Retry-After` truncates to 0 below one second (medium)

`apps/11-clickhouse-sink/internal/server/server.go:199`, in `retryAfterHeader`

What goes wrong: `int64(d/time.Second)` is integer division of a `time.Duration`. With
`FLUSH_INTERVAL=500ms` every 429 carries `Retry-After: 0`, and clients retry immediately, hammering the
service while it is already overloaded. 1.9 s becomes 1.

The tell: a `time.Duration` divided by `time.Second` and converted to an integer, for a value where 0
means "right now".

Fix:

```go
func retryAfterHeader(d time.Duration) string {
	return strconv.FormatInt(max(int64((d+time.Second-1)/time.Second), 1), 10)
}
```

Rule: integer division of durations truncates, so round up and clamp wherever zero changes the meaning.

Found if: the verdict says `Retry-After` becomes 0 (or too small) for sub-second `FLUSH_INTERVAL`
because of the division.

## 4. The "dropped batch" test never drops a batch (medium)

`apps/11-clickhouse-sink/internal/batcher/batcher_test.go:298`, in `TestRunReleasesBytes`

What goes wrong: the case "after a dropped batch" sets `failures: 2`, but `testConfig` allows 3 attempts
(`MaxAttempts: 3`), so the insert succeeds on the third attempt and the batch is stored. The case also
asserts `require.NoError` on `Run`, which a dropped batch would fail. It checks the success path twice
and is what keeps defect 1 green.

The tell: the case name against `failures` < `MaxAttempts`, and a success assertion in a case about a
failure. Nothing looks at `dropped`.

Fix:

```go
		{name: "after a dropped batch", failures: 3},
	...
			err := start(t, b)()
			require.Equal(t, tt.failures >= testConfig().MaxAttempts, err != nil)
			require.Zero(t, b.bytes.Load())
```

Rule: a test for a failure path must show the failure happened, by asserting on it, before it asserts
on the state afterwards.

Found if: the verdict says the dropped case does not drop (2 failures against 3 attempts), or that the
drop path is untested.

## 5. The byte histogram uses the event-count buckets (low)

`apps/11-clickhouse-sink/internal/batcher/batcher.go:81`, in `New`

What goes wrong: `sink_batch_size_bytes` copies `ExponentialBuckets(1, 4, 8)` from
`sink_batch_size_events`, so its top finite bucket is 16 384. A batch of a few thousand events is
hundreds of kilobytes to megabytes, so every observation lands in `+Inf`, and `histogram_quantile`
reports 16 384 for every quantile.

The tell: buckets copied from a metric in a different unit.

Fix:

```go
			Buckets:   prometheus.ExponentialBuckets(1024, 4, 10),
```

Rule: histogram buckets are in the metric's unit; size them for the values you expect to observe.

Found if: the verdict says the bytes histogram's buckets top out far below real batch sizes.

## Not defects

### `len` of a nil `RawMessage`

`apps/11-clickhouse-sink/internal/batcher/batcher.go:237`: `Properties` is nil when a client sends no
properties, and there is no nil check before `len`. In Python `len(None)` raises; in Go `len` of a nil
slice is 0, so an event without properties simply contributes 0 bytes.

### Parallel subtests capturing `tt`

`apps/11-clickhouse-sink/internal/batcher/batcher_test.go:273-274`, and again at 301-302: the closure
passed to `t.Run` calls `t.Parallel()` and uses the loop variable `tt` after the loop has moved on. Before
Go 1.22 every subtest would have seen the last case; since Go 1.22 each iteration has its own `tt`, and
the module is on Go 1.27.

## Also acceptable

- `payloadBytes` counts only the variable-size fields, so for small events it undercounts memory by the
  fixed size of `event.Event` and its channel slot; the event-count bound still applies.
- `Retry-After` of one `FLUSH_INTERVAL` is a poor estimate either way: too short while a flush is
  retrying against a slow ClickHouse, too long when the buffer is full by count and batches go out
  back to back.
- `Run` never cuts a batch by bytes. When events average more than `MaxBytes` / `BATCH_SIZE` (about
  26 KiB with the defaults), the budget fills before a batch is due and every request gets 429 until
  the next tick, and one `INSERT` can carry the whole budget.
- The integration test checks `sink_buffer_length 0` but not `sink_buffer_bytes 0`, which would catch
  accounting drift end to end.
- During shutdown a request that hits the byte budget gets 429 instead of 503, because the byte check
  runs before the `closed` check (the fix for defect 2 removes it).
