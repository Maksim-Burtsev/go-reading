//go:build integration

package pglock_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/pglock"
	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/worker"
)

func TestLockerExclusion(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	instanceA := pglock.New(newPool(t, dsn))
	instanceB := pglock.New(newPool(t, dsn))
	observer := newPool(t, dsn)

	tests := []struct {
		name         string
		holder       *pglock.Locker
		contender    *pglock.Locker
		held         string
		wanted       string
		wantAcquired bool
	}{
		{
			name:      "same job from another instance",
			holder:    instanceA,
			contender: instanceB,
			held:      "purge-expired-sessions",
			wanted:    "purge-expired-sessions",
		},
		{
			name:      "same job from the same instance",
			holder:    instanceA,
			contender: instanceA,
			held:      "purge-expired-sessions",
			wanted:    "purge-expired-sessions",
		},
		{
			name:         "different job from another instance",
			holder:       instanceA,
			contender:    instanceB,
			held:         "purge-expired-sessions",
			wanted:       "rollup-daily-events",
			wantAcquired: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			unlock, acquired, err := tt.holder.TryLock(ctx, tt.held)
			require.NoError(t, err)
			require.True(t, acquired)

			unlockWanted, acquired, err := tt.contender.TryLock(ctx, tt.wanted)
			require.NoError(t, err)
			require.Equal(t, tt.wantAcquired, acquired)
			if acquired {
				require.NoError(t, unlockWanted(ctx))
			}

			require.NoError(t, unlock(ctx))
			require.Zero(t, advisoryLocks(t, observer))

			unlock, acquired, err = tt.contender.TryLock(ctx, tt.held)
			require.NoError(t, err)
			require.True(t, acquired, "lock was not released by its holder")
			require.NoError(t, unlock(ctx))
		})
	}
}

func TestRunnerHoldsLockForTheRun(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	instanceA := pglock.New(newPool(t, dsn))
	instanceB := pglock.New(newPool(t, dsn))
	runner, err := worker.NewRunner(slog.New(slog.DiscardHandler), instanceA, wallClock{}, prometheus.NewRegistry())
	require.NoError(t, err)

	const name = "expire-pending-orders"
	ran := false
	runner.Run(t.Context(), name, jobFunc(func(ctx context.Context) error {
		ran = true
		_, acquired, err := instanceB.TryLock(ctx, name)
		require.NoError(t, err)
		require.False(t, acquired, "lock was not held during the run")
		return nil
	}))
	require.True(t, ran)

	unlock, acquired, err := instanceB.TryLock(t.Context(), name)
	require.NoError(t, err)
	require.True(t, acquired, "lock was not released after the run")
	require.NoError(t, unlock(t.Context()))
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type jobFunc func(ctx context.Context) error

func (f jobFunc) Run(ctx context.Context) error { return f(ctx) }

func startPostgres(t *testing.T) string {
	t.Helper()
	ctr, err := postgres.Run(t.Context(), "postgres:16-alpine", postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	dsn, err := ctr.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)
	return dsn
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func advisoryLocks(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'").Scan(&n)
	require.NoError(t, err)
	return n
}
