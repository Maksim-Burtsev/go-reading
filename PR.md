# store: support Idempotency-Key on order creation

Clients retry `POST /users/{id}/orders` after a timeout, and today every retry places another
order.

This adds optional `Idempotency-Key` support. The key is saved with the order in the same
transaction, in a new `order_idempotency_keys` table keyed by `(user_id, key)`, together with a
hash of the items. A retry with the same key and the same items gets the original order back, with
the same 201 response, instead of placing a new one; reusing a key with other items is rejected
with 422. Items are compared regardless of the order they are listed in. When two requests with the
same key race, the one that loses the key insert answers with the winner's order. Keys longer than
255 bytes are rejected with 400.

The queries are in `orders.sql`, and the sqlc code is regenerated with `go generate`.

Tests: the store integration test covers a retry with the same key, a key reused with other items,
and concurrent retries; the handler tests check that the key reaches the store and that an
oversized key is rejected.
