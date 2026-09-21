// Command kafka-consumer reads JSON events from a Kafka topic as a member of a
// consumer group and writes them to stdout in batches, one JSON line per event.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/consumer"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/retry"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/sink"
)

const (
	retryBaseDelay = 200 * time.Millisecond
	retryMaxDelay  = 5 * time.Second
)

var errInvalidConfig = errors.New("invalid config")

type config struct {
	Brokers         []string      `env:"KAFKA_BROKERS" envDefault:"localhost:9092"`
	Group           string        `env:"KAFKA_GROUP" envDefault:"events-sink"`
	Topic           string        `env:"KAFKA_TOPIC" envDefault:"events"`
	DeadLetterTopic string        `env:"KAFKA_DLQ_TOPIC" envDefault:"events.dlq"`
	BatchSize       int           `env:"BATCH_SIZE" envDefault:"100"`
	BatchTimeout    time.Duration `env:"BATCH_TIMEOUT" envDefault:"1s"`
	MaxAttempts     int           `env:"MAX_ATTEMPTS" envDefault:"3"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, _ []string, getenv func(string) string, stdout, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(stderr, nil))

	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}

	client, err := kgo.NewClient(append(
		consumer.ClientOptions(cfg.Group, cfg.Topic, logger),
		kgo.SeedBrokers(cfg.Brokers...),
	)...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	if err := createTopics(ctx, kadm.NewClient(client), cfg.Topic, cfg.DeadLetterTopic); err != nil {
		return err
	}

	c := consumer.New(
		client,
		sink.NewJSONLines(stdout),
		retry.New(cfg.MaxAttempts, retryBaseDelay, retryMaxDelay),
		consumer.Config{
			DeadLetterTopic: cfg.DeadLetterTopic,
			BatchSize:       cfg.BatchSize,
			BatchTimeout:    cfg.BatchTimeout,
			ShutdownTimeout: cfg.ShutdownTimeout,
		},
		logger,
	)

	logger.InfoContext(ctx, "consumer started",
		"brokers", cfg.Brokers, "group", cfg.Group, "topic", cfg.Topic, "dlq_topic", cfg.DeadLetterTopic)
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	logger.InfoContext(ctx, "consumer stopped")
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
	case len(cfg.Brokers) == 0:
		return config{}, fmt.Errorf("%w: KAFKA_BROKERS is empty", errInvalidConfig)
	case cfg.Topic == cfg.DeadLetterTopic:
		return config{}, fmt.Errorf("%w: KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC", errInvalidConfig)
	case cfg.BatchSize < 1:
		return config{}, fmt.Errorf("%w: BATCH_SIZE must be positive", errInvalidConfig)
	case cfg.BatchTimeout <= 0:
		return config{}, fmt.Errorf("%w: BATCH_TIMEOUT must be positive", errInvalidConfig)
	case cfg.MaxAttempts < 1:
		return config{}, fmt.Errorf("%w: MAX_ATTEMPTS must be positive", errInvalidConfig)
	}
	return cfg, nil
}

func createTopics(ctx context.Context, adm *kadm.Client, topics ...string) error {
	resps, err := adm.CreateTopics(ctx, -1, -1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resps.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}
