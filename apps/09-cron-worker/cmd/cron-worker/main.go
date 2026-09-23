// Command cron-worker runs periodic database maintenance jobs. Before a run,
// the instance takes a PostgreSQL advisory lock named after the job and skips
// the run if another instance holds it, so any number of instances can be
// deployed. At most one instance holds a job's lock at a time, but the job's
// statements run on other connections, so runs can still overlap when the
// lock's connection is lost or a cancelled statement is still executing, and a
// run can repeat right after another instance's. Jobs must tolerate overlapping
// and repeated runs.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/jobs"
	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/migrations"
	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/pglock"
	"github.com/Maksim-Burtsev/go-reading/apps/09-cron-worker/internal/worker"
)

const healthCheckTimeout = 2 * time.Second

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, _ []string, getenv func(string) string, stdout, _ io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := parseConfig(getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	if err := migrations.Up(ctx, pool, logger); err != nil {
		return err
	}

	clock := systemClock{}
	reg := prometheus.NewRegistry()
	runner, err := worker.NewRunner(logger, pglock.New(pool), clock, reg)
	if err != nil {
		return err
	}
	entries := []worker.Entry{
		{
			Name: "purge-expired-sessions",
			Spec: cfg.PurgeSessionsSchedule,
			Job:  jobs.NewPurgeSessions(pool, clock, logger, cfg.SessionPurgeBatchSize),
		},
		{
			Name: "rollup-daily-events",
			Spec: cfg.RollupEventsSchedule,
			Job:  jobs.NewRollupEvents(pool, clock, logger),
		},
		{
			Name: "expire-pending-orders",
			Spec: cfg.ExpireOrdersSchedule,
			Job:  jobs.NewExpireOrders(pool, clock, logger, cfg.PendingOrderTTL),
		},
		{
			Name:       "rollup-recent-events",
			Spec:       cfg.RollupRecentSchedule,
			Job:        newRollupRecent(cfg, pool, clock, logger),
			RunOnStart: true,
		},
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.MetricsAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.MetricsAddr, err)
	}
	srv := &http.Server{
		Handler:           newHandler(reg, pool),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	logger.InfoContext(ctx, "http server listening", "addr", ln.Addr().String())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		return runner.Serve(gctx, entries, cfg.ShutdownTimeout)
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down http server: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	logger.InfoContext(ctx, "shutdown complete")
	return nil
}

// newRollupRecent returns the intraday rollup, or nil when ROLLUP_RECENT_WINDOW
// is 0 and the job is disabled.
func newRollupRecent(cfg config, db jobs.DB, clock jobs.Clock, logger *slog.Logger) *jobs.RollupRecentEvents {
	if cfg.RollupRecentWindow == 0 {
		return nil
	}
	return jobs.NewRollupRecentEvents(db, clock, logger, cfg.RollupRecentWindow)
}

type pinger interface {
	Ping(ctx context.Context) error
}

func newHandler(reg prometheus.Gatherer, db pinger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
