-- +goose Up
CREATE TABLE batches (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status            text        NOT NULL CHECK (status IN ('inserted', 'dead_lettered')),
    record_count      integer     NOT NULL CHECK (record_count > 0),
    dead_letter_count integer     NOT NULL CHECK (dead_letter_count BETWEEN 0 AND record_count),
    insert_duration   interval    NOT NULL,
    partitions        jsonb       NOT NULL CHECK (jsonb_typeof(partitions) = 'array'),
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX batches_created_at_idx ON batches (created_at);

-- +goose Down
DROP TABLE batches;
