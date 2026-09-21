package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

type fakeDB struct {
	affected []int64
	err      error
	calls    [][]any
}

func (db *fakeDB) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	db.calls = append(db.calls, args)
	if db.err != nil {
		return pgconn.CommandTag{}, db.err
	}
	var n int64
	if len(db.affected) > 0 {
		n, db.affected = db.affected[0], db.affected[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
}

type job interface {
	Run(ctx context.Context) error
}

func TestJobsRun(t *testing.T) {
	t.Parallel()

	errDB := errors.New("connection reset")
	now := time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC)
	moscow := time.FixedZone("MSK", 3*60*60)
	discard := slog.New(slog.DiscardHandler)

	tests := []struct {
		name      string
		now       time.Time
		db        fakeDB
		newJob    func(DB, Clock) job
		wantCalls [][]any
		wantErr   error
	}{
		{
			name: "purge sessions repeats while batches are full",
			now:  now,
			db:   fakeDB{affected: []int64{2, 2, 1}},
			newJob: func(db DB, c Clock) job {
				return NewPurgeSessions(db, c, discard, 2)
			},
			wantCalls: [][]any{{now, 2}, {now, 2}, {now, 2}},
		},
		{
			name: "purge sessions with nothing to delete",
			now:  now,
			db:   fakeDB{affected: []int64{0}},
			newJob: func(db DB, c Clock) job {
				return NewPurgeSessions(db, c, discard, 500)
			},
			wantCalls: [][]any{{now, 500}},
		},
		{
			name: "rollup covers the previous UTC day",
			now:  now,
			db:   fakeDB{affected: []int64{4}},
			newJob: func(db DB, c Clock) job {
				return NewRollupEvents(db, c, discard)
			},
			wantCalls: [][]any{{
				pgtype.Date{Time: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC), Valid: true},
				time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC),
			}},
		},
		{
			name: "rollup normalizes a local clock to UTC",
			now:  time.Date(2026, 9, 21, 1, 30, 0, 0, moscow),
			db:   fakeDB{affected: []int64{4}},
			newJob: func(db DB, c Clock) job {
				return NewRollupEvents(db, c, discard)
			},
			wantCalls: [][]any{{
				pgtype.Date{Time: time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC), Valid: true},
				time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
			}},
		},
		{
			name: "expire orders older than the TTL",
			now:  now,
			db:   fakeDB{affected: []int64{3}},
			newJob: func(db DB, c Clock) job {
				return NewExpireOrders(db, c, discard, 45*time.Minute)
			},
			wantCalls: [][]any{{time.Date(2026, 9, 21, 9, 45, 0, 0, time.UTC)}},
		},
		{
			name: "database error stops the purge",
			now:  now,
			db:   fakeDB{err: errDB},
			newJob: func(db DB, c Clock) job {
				return NewPurgeSessions(db, c, discard, 2)
			},
			wantCalls: [][]any{{now, 2}},
			wantErr:   errDB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.newJob(&tt.db, fixedClock(tt.now)).Run(t.Context())

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantCalls, tt.db.calls)
		})
	}
}
