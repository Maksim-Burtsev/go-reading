# Add ReleaseReservation RPC

Checkout abandons carts all the time, and their reservations hold stock until the process restarts.
This adds `ReleaseReservation`: it returns a reservation's quantities to available stock and marks
the reservation released. Releasing twice is a no-op that returns the same reservation, so clients
can retry safely, and an unknown id is NOT_FOUND. Reservations now carry `release_time`, and the
reservation id check moved into `validateID`, shared by Reserve and Release. The Go code in `gen/`
is regenerated with `go generate`.

Tests: `TestReleaseReservation` covers a release, a repeated release, a missing id and an id over
128 bytes through the gRPC server, checking the item's stock afterwards.
`TestReleaseReservationConcurrentRetries` sends 20 retries of the same release and checks the stock
comes back exactly once.
