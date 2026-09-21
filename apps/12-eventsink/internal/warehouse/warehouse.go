// Package warehouse writes events to ClickHouse over the native protocol.
package warehouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/event"
)

const createEventsTable = `
CREATE TABLE IF NOT EXISTS events (
    event_id    String,
    event_type  LowCardinality(String),
    source      LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    payload     String CODEC(ZSTD(3)),
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_type, occurred_at, event_id)`

const insertEvents = `INSERT INTO events (event_id, event_type, source, occurred_at, payload)`

// Store is a ClickHouse-backed event store. It is safe for concurrent use.
type Store struct {
	conn driver.Conn
}

// Open connects to the ClickHouse server at dsn and creates the events table
// if it does not exist. The table collapses rows of the same event during
// background merges, so a replayed batch leaves one row per event.
func Open(ctx context.Context, dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Exec(ctx, createEventsTable); err != nil {
		return nil, errors.Join(fmt.Errorf("create events table: %w", err), conn.Close())
	}
	return &Store{conn: conn}, nil
}

// InsertEvents writes events in a single INSERT block.
func (s *Store) InsertEvents(ctx context.Context, events []event.Event) (err error) {
	if len(events) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, insertEvents)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer func() {
		if cerr := batch.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close batch: %w", cerr))
		}
	}()

	for i := range events {
		e := &events[i]
		if err := batch.Append(e.ID, e.Type, e.Source, e.OccurredAt, string(e.Payload)); err != nil {
			return fmt.Errorf("append event %s: %w", e.ID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch of %d events: %w", len(events), err)
	}
	return nil
}

// Ping checks that ClickHouse is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	return nil
}

// Close releases all connections.
func (s *Store) Close() error {
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("close clickhouse: %w", err)
	}
	return nil
}
