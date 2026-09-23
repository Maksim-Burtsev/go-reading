// Command eventsink consumes JSON events from a Kafka topic, writes them to
// ClickHouse in batches and records every batch in Postgres before it commits
// the batch's offsets.
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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/ledger"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/pipeline"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/server"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/warehouse"
)

var errInvalidConfig = errors.New("invalid config")

type config struct {
	Addr            string        `env:"ADDR"             envDefault:":8012"`
	KafkaBrokers    []string      `env:"KAFKA_BROKERS"    envDefault:"localhost:19012"`
	KafkaGroup      string        `env:"KAFKA_GROUP"      envDefault:"eventsink"`
	KafkaTopic      string        `env:"KAFKA_TOPIC"      envDefault:"events"`
	KafkaDLQTopic   string        `env:"KAFKA_DLQ_TOPIC"  envDefault:"events.dlq"`
	ClickHouseURL   string        `env:"CLICKHOUSE_URL"   envDefault:"clickhouse://eventsink:eventsink@localhost:9012/eventsink?dial_timeout=5s&compress=lz4"`
	DatabaseURL     string        `env:"DATABASE_URL"     envDefault:"postgres://eventsink:eventsink@localhost:5412/eventsink?sslmode=disable"`
	BatchSize       int           `env:"BATCH_SIZE"       envDefault:"1000"`
	BatchTimeout    time.Duration `env:"BATCH_TIMEOUT"    envDefault:"2s"`
	MaxAttempts     int           `env:"MAX_ATTEMPTS"     envDefault:"5"`
	RetryBackoff    time.Duration `env:"RETRY_BACKOFF"    envDefault:"200ms"`
	AttemptTimeout  time.Duration `env:"ATTEMPT_TIMEOUT"  envDefault:"3s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()
	if err := ledger.Migrate(ctx, pool); err != nil {
		return err
	}

	store, err := warehouse.Open(ctx, cfg.ClickHouseURL)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.ErrorContext(ctx, "close clickhouse", "error", err)
		}
	}()

	client, err := kgo.NewClient(append(pipeline.ClientOptions(cfg.KafkaGroup, cfg.KafkaTopic), kgo.SeedBrokers(cfg.KafkaBrokers...))...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	reg := prometheus.NewRegistry()
	p, err := pipeline.New(client, store, ledger.New(pool), pipeline.Config{
		DeadLetterTopic: cfg.KafkaDLQTopic,
		BatchSize:       cfg.BatchSize,
		BatchTimeout:    cfg.BatchTimeout,
		MaxAttempts:     cfg.MaxAttempts,
		RetryBackoff:    cfg.RetryBackoff,
		AttemptTimeout:  cfg.AttemptTimeout,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, reg, logger)
	if err != nil {
		return err
	}

	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}
	srv := &http.Server{
		Handler: server.NewHandler(logger, reg, map[string]server.Pinger{
			"clickhouse": store,
			"postgres":   pool,
			"kafka":      client,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		defer cancel()
		serveErr <- srv.Serve(ln)
	}()
	logger.InfoContext(ctx, "eventsink started", "addr", ln.Addr().String(), "group", cfg.KafkaGroup, "topic", cfg.KafkaTopic)

	var errs []error
	if err := p.Run(ctx); err != nil {
		errs = append(errs, fmt.Errorf("run pipeline: %w", err))
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("shutdown http: %w", err))
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("serve http: %w", err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
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
		if v := getenv(p.Key); v != "" {
			environ[p.Key] = v
		}
	}
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}

	switch {
	case len(cfg.KafkaBrokers) == 0:
		return config{}, fmt.Errorf("%w: KAFKA_BROKERS is empty", errInvalidConfig)
	case cfg.KafkaTopic == cfg.KafkaDLQTopic:
		return config{}, fmt.Errorf("%w: KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC", errInvalidConfig)
	case cfg.BatchSize < 1:
		return config{}, fmt.Errorf("%w: BATCH_SIZE must be positive", errInvalidConfig)
	case cfg.BatchTimeout <= 0:
		return config{}, fmt.Errorf("%w: BATCH_TIMEOUT must be positive", errInvalidConfig)
	case cfg.MaxAttempts < 1:
		return config{}, fmt.Errorf("%w: MAX_ATTEMPTS must be positive", errInvalidConfig)
	case cfg.AttemptTimeout <= 0:
		return config{}, fmt.Errorf("%w: ATTEMPT_TIMEOUT must be positive", errInvalidConfig)
	}
	return cfg, nil
}
