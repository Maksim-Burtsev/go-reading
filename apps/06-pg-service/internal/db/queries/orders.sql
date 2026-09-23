-- name: CreateOrder :one
INSERT INTO orders (user_id)
VALUES ($1)
RETURNING id, user_id, total_cents, created_at;

-- name: InsertOrderItems :copyfrom
INSERT INTO order_items (order_id, sku, quantity, unit_price_cents)
VALUES ($1, $2, $3, $4);

-- name: UpdateOrderTotal :one
UPDATE orders
SET total_cents = (
    SELECT coalesce(sum(i.quantity * i.unit_price_cents), 0)
    FROM order_items i
    WHERE i.order_id = orders.id
)
WHERE orders.id = $1
RETURNING id, user_id, total_cents, created_at;

-- name: ListOrders :many
SELECT id, user_id, total_cents, created_at
FROM orders
WHERE user_id = @user_id
ORDER BY created_at DESC, id DESC
LIMIT @row_limit::bigint;

-- name: ListOrdersAfter :many
SELECT id, user_id, total_cents, created_at
FROM orders
WHERE user_id = @user_id
  AND (created_at, id) < (@after_created_at::timestamptz, @after_id::bigint)
ORDER BY created_at DESC, id DESC
LIMIT @row_limit::bigint;

-- name: GetOrderByIdempotencyKey :one
SELECT o.id, o.user_id, o.total_cents, o.created_at, k.request_hash
FROM order_idempotency_keys k
JOIN orders o ON o.id = k.order_id
WHERE k.key = $1;

-- name: InsertIdempotencyKey :exec
INSERT INTO order_idempotency_keys (user_id, key, request_hash, order_id)
VALUES ($1, $2, $3, $4);
