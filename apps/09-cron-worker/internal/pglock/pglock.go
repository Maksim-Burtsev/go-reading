// Package pglock provides non-blocking named locks backed by PostgreSQL
// session-level advisory locks.
package pglock

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const discardTimeout = time.Second

// ErrNotHeld is returned when a lock is released by a session that no longer
// holds it.
var ErrNotHeld = errors.New("advisory lock not held by session")

// Locker takes advisory locks on connections checked out of a pool.
type Locker struct {
	pool *pgxpool.Pool
}

// New returns a Locker backed by pool.
func New(pool *pgxpool.Pool) *Locker {
	return &Locker{pool: pool}
}

// Key derives the advisory lock key for name.
func Key(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64() & math.MaxInt64)
}

// TryLock takes the lock called name without waiting. A session-level advisory
// lock belongs to the connection that took it, so the connection stays checked
// out of the pool until unlock is called. When another session holds the lock,
// acquired is false and err is nil.
func (l *Locker) TryLock(ctx context.Context, name string) (unlock func(context.Context) error, acquired bool, err error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire connection: %w", err)
	}

	key := Key(name)
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		discard(ctx, conn)
		return nil, false, fmt.Errorf("try advisory lock %q: %w", name, err)
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}

	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&released); err != nil {
			discard(ctx, conn)
			return fmt.Errorf("advisory unlock %q: %w", name, err)
		}
		conn.Release()
		if !released {
			return fmt.Errorf("advisory unlock %q: %w", name, ErrNotHeld)
		}
		return nil
	}, true, nil
}

func discard(ctx context.Context, conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), discardTimeout)
	defer cancel()
	_ = conn.Hijack().Close(ctx)
}
