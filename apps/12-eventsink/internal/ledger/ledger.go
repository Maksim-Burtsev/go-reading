// Package ledger records every flushed batch in Postgres, together with the
// offsets it covered on each partition.
package ledger

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

const insertBatch = `
INSERT INTO batches (status, record_count, dead_letter_count, insert_duration, partitions)
VALUES ($1, $2, $3, $4, $5)`

//go:embed migrations/*.sql
var migrations embed.FS

// Status is the outcome of a flushed batch.
type Status string

// Batch outcomes.
const (
	StatusInserted              Status = "inserted"
	StatusPartiallyDeadLettered Status = "partially_dead_lettered"
	StatusDeadLettered          Status = "dead_lettered"
)

// Batch describes one flushed batch.
type Batch struct {
	Status         Status
	Records        int
	DeadLettered   int
	InsertDuration time.Duration
	Partitions     []Partition
}

// Partition is the span of offsets a batch took from one topic partition.
type Partition struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	FirstOffset int64  `json:"first_offset"`
	LastOffset  int64  `json:"last_offset"`
	Records     int    `json:"records"`
}

// Ledger is the Postgres-backed batch log.
type Ledger struct {
	pool *pgxpool.Pool
}

// New returns a Ledger that writes through pool.
func New(pool *pgxpool.Pool) *Ledger {
	return &Ledger{pool: pool}
}

// Migrate applies all pending migrations. A Postgres advisory lock serializes
// concurrent callers, so every replica may run it on startup.
func Migrate(ctx context.Context, pool *pgxpool.Pool) (err error) {
	fsys, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("open migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create session locker: %w", err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() {
		err = errors.Join(err, sqlDB.Close())
	}()

	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// Record stores b as one row, with its partitions in a JSON array.
func (l *Ledger) Record(ctx context.Context, b Batch) error {
	if _, err := l.pool.Exec(ctx, insertBatch, b.Status, b.Records, b.DeadLettered, b.InsertDuration, b.Partitions); err != nil {
		return fmt.Errorf("insert batch: %w", err)
	}
	return nil
}
