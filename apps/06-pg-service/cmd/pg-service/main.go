// Command pg-service serves the users and orders HTTP API backed by Postgres.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/db"
	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/httpapi"
	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

type config struct {
	Addr            string         `env:"ADDR" envDefault:":8080"`
	DatabaseURL     string         `env:"DATABASE_URL" envDefault:"postgres://orders:orders@localhost:5406/orders?sslmode=disable"`
	ShutdownTimeout time.Duration  `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
	TrustedProxies  []netip.Prefix `env:"TRUSTED_PROXIES"`
}

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, _ []string, getenv func(string) string, _, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(stderr, nil))

	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	applied, err := db.Migrate(ctx, pool)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "migrations applied", slog.Any("versions", applied))

	srv := &http.Server{
		Handler:           httpapi.NewHandler(logger, store.New(pool), cfg.TrustedProxies),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	logger.InfoContext(ctx, "listening", slog.String("addr", ln.Addr().String()))

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	logger.InfoContext(ctx, "shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	logger.InfoContext(ctx, "stopped")
	return nil
}

func loadConfig(getenv func(string) string) (config, error) {
	var cfg config
	params, err := env.GetFieldParams(&cfg)
	if err != nil {
		return config{}, fmt.Errorf("inspect config: %w", err)
	}

	environ := make(map[string]string, len(params))
	for _, p := range params {
		environ[p.Key] = getenv(p.Key)
	}

	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
