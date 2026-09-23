-- +goose Up
CREATE TABLE order_idempotency_keys (
    user_id      bigint      NOT NULL REFERENCES users (id),
    key          text        NOT NULL,
    request_hash text        NOT NULL,
    order_id     bigint      NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);

-- +goose Down
DROP TABLE order_idempotency_keys;
