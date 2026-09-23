# 008 · Add ReleaseReservation RPC

5 defects, 2 decoys.

## 1. Release checks and acts in two critical sections (high)

`apps/08-grpc-service/internal/inventory/store.go:188`, in `Store.Release`

What goes wrong: two releases of the same reservation overlap. That happens when a client retries after
its deadline while the first call is still inside `Release`, or when checkout and a cart-expiry job
release the same order at once. Each call runs `s.reservation`, which locks, copies the reservation
with a zero `ReleasedAt`, and unlocks. The check at line 188 then runs with no lock held, so both calls
see "not released", and each takes the lock again at line 192 and returns the stock. Reserve 2 of
BOOK-1 (3 in stock) and release it twice concurrently: Available ends at 5 and Reserved at -2, so the
next Reserve sells stock that does not exist. `-race` stays silent, because every map access is under
the lock; the race is on the decision, not on memory. With the in-memory store the gap between the two
locks is short: 8 simultaneous `Store.Release` calls returned the stock twice in 2 of 1000 rounds. A
slower store, a GC pause or a preempted goroutine widens it.

The tell: one operation takes `s.mu` twice. `s.reservation` locks and unlocks, the decision is made in
between, and the write happens under a second `Lock`. `Reserve`, a few lines above, holds the lock from
lookup to commit.

Fix:

```go
func (s *Store) Release(ctx context.Context, id string) (Reservation, error) {
	if err := validateID(id); err != nil {
		return Reservation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Reservation{}, fmt.Errorf("release %s: %w", id, err)
	}
	r, ok := s.reservations[id]
	if !ok {
		return Reservation{}, fmt.Errorf("release %s: %w", id, ErrReservationNotFound)
	}
	if !r.ReleasedAt.IsZero() {
		return r.clone(), nil
	}
	for _, line := range r.Lines {
		item := s.items[line.SKU]
		item.Available += line.Quantity
		item.Reserved -= line.Quantity
		s.items[line.SKU] = item
	}
	r = r.released(s.now())
	s.reservations[id] = r
	return r.clone(), nil
}
```

Rule: a check and the write that depends on it belong under one lock or in one transaction, because
taking the lock again does not re-validate what was read before.

Found if: the verdict names `Store.Release` (the separate `reservation` lookup, or the second
`Lock`) and says two concurrent releases can both pass the released check and return the stock twice.

## 2. A zero time becomes a present `release_time` (medium)

`apps/08-grpc-service/internal/server/service.go:127`, in `toProtoReservation`

What goes wrong: `timestamppb.New(time.Time{})` does not return nil. It returns a Timestamp for
0001-01-01T00:00:00Z. Every reservation that is not released, including every Reserve response, now has
`release_time` set, and grpcurl prints `"releaseTime": "0001-01-01T00:00:00Z"`. A client that checks
`GetReleaseTime() != nil`, or `HasField("release_time")` in Python, treats every live reservation as
released.

The tell: `ReleasedAt` is zero by design (the struct comment says so), yet it is converted with no
`IsZero` check, while the proto comment says the field is set only once the reservation is released. The
tests assert `NotNil` only on release responses.

Fix:

```go
	pr := &inventoryv1.Reservation{
		Id:         r.ID,
		Lines:      lines,
		CreateTime: timestamppb.New(r.CreatedAt),
	}
	if !r.ReleasedAt.IsZero() {
		pr.ReleaseTime = timestamppb.New(r.ReleasedAt)
	}
	return pr
```

Rule: a Go zero value is a value, not an absence, so map it to an unset field or a nil pointer
explicitly.

Found if: the verdict names the unconditional `timestamppb.New(r.ReleasedAt)` and says unreleased
reservations come back with a `release_time`.

## 3. The new sentinel never reaches a status code (medium)

`apps/08-grpc-service/internal/server/service.go:103`, in `toStatus`

What goes wrong: call `ReleaseReservation` with an id that was never reserved. `Store.Release`
returns `release nope: reservation not found`, which wraps the new `ErrReservationNotFound`. `toStatus`
has no case for it, so it falls through to `default` and the client gets INTERNAL instead of the
NOT_FOUND that the proto comment in the same diff, and PR.md, promise. Clients and retry policies treat
INTERNAL as a server fault and retry a call that can never succeed, and the logging interceptor records
each one as `rpc failed` at ERROR.

The tell: a new exported sentinel and a newly documented NOT_FOUND, with no change to `toStatus`, the one
function that maps sentinels to codes. The gRPC test table has no unknown-id case.

Fix:

```go
	case errors.Is(err, inventory.ErrNotFound), errors.Is(err, inventory.ErrReservationNotFound):
		code = codes.NotFound
```

Rule: when a change adds an error, follow it to every place that translates errors (status codes, HTTP
responses, metrics) and test the translation, not just the error.

Found if: the verdict says an unknown reservation id reaches the client as something other than
NOT_FOUND (INTERNAL) because `ErrReservationNotFound` is not mapped in `toStatus`.

## 4. The concurrency test runs its retries one at a time (low)

`apps/08-grpc-service/internal/server/server_test.go:423`, in `TestReleaseReservationConcurrentRetries`

What goes wrong: `wg.Wait()` sits inside the loop that starts the goroutines, so each release finishes
before the next one starts. A test named for concurrent retries sends 20 sequential ones and passes with
defect 1 in place, and no interleaving bug can ever make it fail. Moving the wait out makes the calls
overlap, but even then it catches defect 1 only occasionally: a race test raises the odds, and the
single critical section is the actual fix.

The tell: `wg.Wait()` inside the `for` loop, right after `wg.Go`.

Fix:

```go
	for i := range retries {
		wg.Go(func() {
			_, errs[i] = client.ReleaseReservation(t.Context(), &inventoryv1.ReleaseReservationRequest{ReservationId: "r1"})
		})
	}
	wg.Wait()
```

Rule: a concurrency test has to let the calls overlap, and waiting inside the loop that starts them
turns it into a sequential test.

Found if: the verdict names the `wg.Wait()` inside the loop and says the retries never run
concurrently.

## 5. The moved id check rejects exactly 128 bytes (medium)

`apps/08-grpc-service/internal/inventory/store.go:235`, in `validateID`

What goes wrong: `validateID` uses `>=` where `normalize` had `>`. A reservation id of exactly 128 bytes
is valid by the proto, which calls only "longer than 128 bytes" invalid, and was accepted before this
change. Both Reserve and Release now reject it with `invalid reservation: id longer than 128 bytes`,
which is not even true. A client that builds ids from two SHA-256 hex digests (128 characters) starts
failing after the deploy.

The tell: `>` became `>=` in code the PR describes as moved, while the message still says "longer
than". The only length test uses 129 bytes.

Fix:

```go
	case len(id) > maxReservationIDLen:
```

Rule: diff moved code against the original line by line, and put boundary tests at the limit itself
(128 accepted, 129 rejected).

Found if: the verdict names the `>=` in `validateID` and says a 128-byte id is now rejected.

## Not defects

### A value receiver that assigns to its receiver

`apps/08-grpc-service/internal/inventory/store.go:226`: `released` assigns `r.ReleasedAt` on a value
receiver, which reads like a lost update to someone used to Python objects passed by reference. It
returns the modified copy, and the caller assigns `r = r.released(s.now())` and stores `r` in the map,
so nothing is lost. `clone` next to it works the same way.

### Goroutines that read the loop variable

`apps/08-grpc-service/internal/server/server_test.go:421`: the closure passed to `wg.Go` reads `i`,
which a Python reader expects to be late-bound to its last value. Since Go 1.22 each iteration of a
`for` loop has its own `i`, so every goroutine writes its own element of `errs`, and writes to different
elements of one slice do not race.

## Also acceptable

- A Reserve retry that arrives after Release, with the same id and lines, returns OK and the released
  reservation, so it looks successful while holding no stock. With defect 2 fixed, `release_time` tells
  the client; the id can never be reused.
- Reserve/release cycles now grow `s.reservations` without bound. Before this change every stored
  reservation held stock, so the map was bounded by the total stock.
- `Store.Release` has no store-level tests; everything is tested through gRPC.
- The `NewStore` doc still says `now` stamps new reservations; it now stamps releases too.
