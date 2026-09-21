// Package storage writes events to ClickHouse over the native protocol.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
)

const createEventsTable = `
CREATE TABLE IF NOT EXISTS events (
    event_id    UUID,
    event_type  LowCardinality(String),
    user_id     String,
    ts          DateTime64(3, 'UTC'),
    properties  String CODEC(ZSTD(3)),
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (event_type, ts)`

const insertEvents = `INSERT INTO events (event_id, event_type, user_id, ts, properties)`

// Config holds the ClickHouse connection settings.
type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

// Store is a ClickHouse-backed event store. It is safe for concurrent use.
type Store struct {
	conn driver.Conn
}

// Open connects to ClickHouse and verifies the connection with a ping.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("ping clickhouse: %w", err), conn.Close())
	}
	return &Store{conn: conn}, nil
}

// Migrate creates the events table if it does not exist.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.conn.Exec(ctx, createEventsTable); err != nil {
		return fmt.Errorf("create events table: %w", err)
	}
	return nil
}

// InsertEvents writes events in a single INSERT block.
func (s *Store) InsertEvents(ctx context.Context, events []event.Event) (err error) {
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
		if err := batch.Append(e.ID, e.Type, e.UserID, e.Timestamp, e.PropertiesJSON()); err != nil {
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
