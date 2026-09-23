// Package warehouse writes events to ClickHouse over the native protocol.
package warehouse

import (
	"context"
	"errors"
	"fmt"
	"net/url"

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

// errInvalidDSN replaces the driver's error for a DSN that is not a URL, which
// quotes the whole DSN, password included.
var errInvalidDSN = errors.New("parse clickhouse dsn: not a valid URL")

// Store is a ClickHouse-backed event store. It is safe for concurrent use.
type Store struct {
	conn driver.Conn
}

// Open connects to the ClickHouse server at dsn and creates the events table
// if it does not exist. Rows of the same event collapse into one when
// ClickHouse merges the parts that hold them, or at query time with FINAL;
// until then a plain SELECT can see a replayed event twice.
func Open(ctx context.Context, dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		if _, ok := errors.AsType[*url.Error](err); ok {
			return nil, errInvalidDSN
		}
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
