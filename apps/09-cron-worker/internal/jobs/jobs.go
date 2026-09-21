// Package jobs implements the periodic maintenance jobs run by the worker.
package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// DB executes SQL statements.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Clock reports the current time.
type Clock interface {
	Now() time.Time
}

const purgeSessionsSQL = `
DELETE FROM sessions
WHERE id IN (
	SELECT id
	FROM sessions
	WHERE expires_at < $1
	ORDER BY expires_at
	LIMIT $2
	FOR UPDATE SKIP LOCKED
)`

// PurgeSessions deletes expired sessions in batches so that no single
// statement holds row locks on a large part of the table.
type PurgeSessions struct {
	db        DB
	clock     Clock
	logger    *slog.Logger
	batchSize int
}

// NewPurgeSessions returns a PurgeSessions job deleting up to batchSize rows
// per statement. batchSize must be positive.
func NewPurgeSessions(db DB, clock Clock, logger *slog.Logger, batchSize int) *PurgeSessions {
	return &PurgeSessions{db: db, clock: clock, logger: logger, batchSize: batchSize}
}

// Run deletes every session that expired before the run started.
func (j *PurgeSessions) Run(ctx context.Context) error {
	cutoff := j.clock.Now()
	var total int64
	for {
		tag, err := j.db.Exec(ctx, purgeSessionsSQL, cutoff, j.batchSize)
		if err != nil {
			return fmt.Errorf("delete expired sessions: %w", err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < int64(j.batchSize) {
			break
		}
	}
	j.logger.InfoContext(ctx, "expired sessions purged", "count", total, "cutoff", cutoff)
	return nil
}

const rollupEventsSQL = `
INSERT INTO daily_event_stats (day, event_type, event_count, unique_users)
SELECT $1::date, event_type, count(*), count(DISTINCT user_id)
FROM events
WHERE occurred_at >= $2 AND occurred_at < $3
GROUP BY event_type
ON CONFLICT (day, event_type) DO UPDATE
SET event_count = EXCLUDED.event_count,
    unique_users = EXCLUDED.unique_users,
    computed_at = now()`

// RollupEvents aggregates the previous UTC day of events into
// daily_event_stats. Re-running it for the same day overwrites the totals.
type RollupEvents struct {
	db     DB
	clock  Clock
	logger *slog.Logger
}

// NewRollupEvents returns a RollupEvents job.
func NewRollupEvents(db DB, clock Clock, logger *slog.Logger) *RollupEvents {
	return &RollupEvents{db: db, clock: clock, logger: logger}
}

// Run computes per-type event counts and unique users for yesterday in UTC.
func (j *RollupEvents) Run(ctx context.Context) error {
	y, m, d := j.clock.Now().UTC().Date()
	to := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	from := to.AddDate(0, 0, -1)

	tag, err := j.db.Exec(ctx, rollupEventsSQL, pgtype.Date{Time: from, Valid: true}, from, to)
	if err != nil {
		return fmt.Errorf("roll up events for %s: %w", from.Format(time.DateOnly), err)
	}
	j.logger.InfoContext(ctx, "daily event stats rolled up",
		"day", from.Format(time.DateOnly), "event_types", tag.RowsAffected())
	return nil
}

const expireOrdersSQL = `
UPDATE orders
SET status = 'expired', updated_at = now()
WHERE status = 'pending' AND created_at < $1`

// ExpireOrders marks orders that stayed pending for longer than a TTL as
// expired.
type ExpireOrders struct {
	db     DB
	clock  Clock
	logger *slog.Logger
	ttl    time.Duration
}

// NewExpireOrders returns an ExpireOrders job for orders pending longer than
// ttl.
func NewExpireOrders(db DB, clock Clock, logger *slog.Logger, ttl time.Duration) *ExpireOrders {
	return &ExpireOrders{db: db, clock: clock, logger: logger, ttl: ttl}
}

// Run expires every pending order created before now minus the TTL.
func (j *ExpireOrders) Run(ctx context.Context) error {
	cutoff := j.clock.Now().Add(-j.ttl)
	tag, err := j.db.Exec(ctx, expireOrdersSQL, cutoff)
	if err != nil {
		return fmt.Errorf("expire pending orders: %w", err)
	}
	j.logger.InfoContext(ctx, "stale pending orders expired", "count", tag.RowsAffected(), "cutoff", cutoff)
	return nil
}
