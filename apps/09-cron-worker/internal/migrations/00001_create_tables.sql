-- +goose Up
CREATE TABLE sessions (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX sessions_expires_at_idx ON sessions (expires_at);

CREATE TABLE events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     bigint      NOT NULL,
    event_type  text        NOT NULL,
    occurred_at timestamptz NOT NULL
);

CREATE INDEX events_occurred_at_idx ON events (occurred_at);

CREATE TABLE daily_event_stats (
    day          date        NOT NULL,
    event_type   text        NOT NULL,
    event_count  bigint      NOT NULL,
    unique_users bigint      NOT NULL,
    computed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, event_type)
);

CREATE TABLE orders (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status     text        NOT NULL CHECK (status IN ('pending', 'paid', 'expired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX orders_pending_created_at_idx ON orders (created_at) WHERE status = 'pending';

-- +goose Down
DROP TABLE orders;
DROP TABLE daily_event_stats;
DROP TABLE events;
DROP TABLE sessions;
