-- +goose Up
CREATE TABLE orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     bigint      NOT NULL REFERENCES users (id),
    total_cents bigint      NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX orders_user_id_created_at_id_idx ON orders (user_id, created_at DESC, id DESC);

CREATE TABLE order_items (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id         bigint  NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    sku              text    NOT NULL CHECK (sku <> ''),
    quantity         integer NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint  NOT NULL CHECK (unit_price_cents >= 0)
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);

-- +goose Down
DROP TABLE order_items;
DROP TABLE orders;
