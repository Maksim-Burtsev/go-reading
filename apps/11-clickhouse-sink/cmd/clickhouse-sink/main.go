// Command clickhouse-sink accepts analytics events over HTTP, buffers them in
// memory and writes them to ClickHouse in batches.
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

	"github.com/caarlos0/env/v11"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/batcher"
	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/server"
	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/storage"
)

var errInvalidConfig = errors.New("invalid config")

type config struct {
	Addr               string        `env:"ADDR"                envDefault:":8080"`
	ClickHouseAddr     string        `env:"CLICKHOUSE_ADDR"     envDefault:"localhost:9000"`
	ClickHouseDB       string        `env:"CLICKHOUSE_DB"       envDefault:"sink"`
	ClickHouseUser     string        `env:"CLICKHOUSE_USER"     envDefault:"sink"`
	ClickHousePassword string        `env:"CLICKHOUSE_PASSWORD" envDefault:"sink"`
	BufferSize         int           `env:"BUFFER_SIZE"         envDefault:"100000"`
	BatchSize          int           `env:"BATCH_SIZE"          envDefault:"10000"`
	FlushInterval      time.Duration `env:"FLUSH_INTERVAL"      envDefault:"1s"`
	FlushMaxAttempts   int           `env:"FLUSH_MAX_ATTEMPTS"  envDefault:"3"`
	FlushRetryBackoff  time.Duration `env:"FLUSH_RETRY_BACKOFF" envDefault:"200ms"`
	ShutdownTimeout    time.Duration `env:"SHUTDOWN_TIMEOUT"    envDefault:"15s"`
}

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, _ []string, getenv func(string) string, stdout, _ io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}

	store, err := storage.Open(ctx, storage.Config{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		Username: cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.ErrorContext(ctx, "close storage", "error", err)
		}
	}()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	for _, c := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("register runtime metrics: %w", err)
		}
	}

	buf, err := batcher.New(batcher.Config{
		BufferSize:    cfg.BufferSize,
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		MaxAttempts:   cfg.FlushMaxAttempts,
		RetryBackoff:  cfg.FlushRetryBackoff,
		DrainTimeout:  cfg.ShutdownTimeout,
	}, store, reg, logger)
	if err != nil {
		return err
	}

	handler, err := server.NewHandler(logger, buf, store, reg)
	if err != nil {
		return err
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	logger.InfoContext(ctx, "listening", "addr", ln.Addr().String())
	return serve(ctx, logger, srv, ln, buf, cfg.ShutdownTimeout)
}

func serve(
	ctx context.Context,
	logger *slog.Logger,
	srv *http.Server,
	ln net.Listener,
	buf *batcher.Batcher,
	shutdownTimeout time.Duration,
) error {
	batcherCtx, stopBatcher := context.WithCancel(context.WithoutCancel(ctx))
	defer stopBatcher()

	batcherDone := make(chan error, 1)
	go func() { batcherDone <- buf.Run(batcherCtx) }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		stopBatcher()
		return errors.Join(fmt.Errorf("serve http: %w", err), <-batcherDone)
	case <-ctx.Done():
	}

	logger.InfoContext(ctx, "shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	var errs []error
	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("shutdown http: %w", err))
	}
	stopBatcher()
	if err := <-batcherDone; err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		logger.InfoContext(ctx, "stopped")
	}
	return errors.Join(errs...)
}

func loadConfig(getenv func(string) string) (config, error) {
	var cfg config
	params, err := env.GetFieldParams(&cfg)
	if err != nil {
		return config{}, fmt.Errorf("inspect config: %w", err)
	}
	environ := make(map[string]string, len(params))
	for _, p := range params {
		if v := getenv(p.Key); v != "" {
			environ[p.Key] = v
		}
	}
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}

	switch {
	case cfg.BatchSize <= 0:
		return config{}, fmt.Errorf("%w: BATCH_SIZE must be positive", errInvalidConfig)
	case cfg.BufferSize < cfg.BatchSize:
		return config{}, fmt.Errorf("%w: BUFFER_SIZE must be at least BATCH_SIZE", errInvalidConfig)
	case cfg.FlushInterval <= 0:
		return config{}, fmt.Errorf("%w: FLUSH_INTERVAL must be positive", errInvalidConfig)
	case cfg.FlushMaxAttempts <= 0:
		return config{}, fmt.Errorf("%w: FLUSH_MAX_ATTEMPTS must be positive", errInvalidConfig)
	}
	return cfg, nil
}
