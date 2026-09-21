//go:build integration

package jobs_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/jobs"
	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/migrations"
)

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

func TestJobsAgainstPostgres(t *testing.T) {
	t.Parallel()

	pool := migratedPool(t)
	clock := fixedClock(time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC))
	discard := slog.New(slog.DiscardHandler)

	tests := []struct {
		name  string
		seed  string
		job   interface{ Run(context.Context) error }
		query string
		want  string
	}{
		{
			name: "purge deletes only expired sessions",
			seed: `INSERT INTO sessions (user_id, expires_at) VALUES
				(1, '2026-09-21 10:29:59+00'),
				(2, '2026-09-20 00:00:00+00'),
				(3, '2026-01-01 00:00:00+00'),
				(4, '2026-09-21 10:30:00+00'),
				(5, '2026-09-22 00:00:00+00')`,
			job:   jobs.NewPurgeSessions(pool, clock, discard, 2),
			query: `SELECT string_agg(user_id::text, ',' ORDER BY user_id) FROM sessions`,
			want:  "4,5",
		},
		{
			name: "rollup aggregates yesterday's events only",
			seed: `INSERT INTO events (user_id, event_type, occurred_at) VALUES
				(1, 'login', '2026-09-20 00:00:00+00'),
				(1, 'login', '2026-09-20 12:00:00+00'),
				(2, 'login', '2026-09-20 23:59:59+00'),
				(3, 'purchase', '2026-09-20 08:00:00+00'),
				(4, 'login', '2026-09-19 23:59:59+00'),
				(5, 'purchase', '2026-09-21 00:00:00+00')`,
			job: jobs.NewRollupEvents(pool, clock, discard),
			query: `SELECT string_agg(format('%s %s %s/%s', day, event_type, event_count, unique_users), ','
				ORDER BY day, event_type) FROM daily_event_stats`,
			want: "2026-09-20 login 3/2,2026-09-20 purchase 1/1",
		},
		{
			name: "expire touches only stale pending orders",
			seed: `INSERT INTO orders (status, created_at) VALUES
				('pending', '2026-09-21 09:00:00+00'),
				('pending', '2026-09-21 10:15:00+00'),
				('paid', '2026-09-21 08:00:00+00')`,
			job:   jobs.NewExpireOrders(pool, clock, discard, 30*time.Minute),
			query: `SELECT string_agg(status, ',' ORDER BY id) FROM orders`,
			want:  "expired,pending,paid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			_, err := pool.Exec(ctx, tt.seed)
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, tt.job.Run(ctx))
			}

			var got string
			require.NoError(t, pool.QueryRow(ctx, tt.query).Scan(&got))
			require.Equal(t, tt.want, got)
		})
	}
}

func migratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine", postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, migrations.Up(ctx, pool, slog.New(slog.DiscardHandler)))
	return pool
}
