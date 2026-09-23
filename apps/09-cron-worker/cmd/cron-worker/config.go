package main

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/caarlos0/env/v11"
)

var errInvalidConfig = errors.New("invalid config")

type config struct {
	DatabaseURL     string        `env:"DATABASE_URL" envDefault:"postgres://cron:cron@localhost:5432/cron?sslmode=disable"`
	MetricsAddr     string        `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel        slog.Level    `env:"LOG_LEVEL" envDefault:"info"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`

	PurgeSessionsSchedule string `env:"PURGE_SESSIONS_SCHEDULE" envDefault:"0 */5 * * * *"`
	RollupEventsSchedule  string `env:"ROLLUP_EVENTS_SCHEDULE" envDefault:"0 15 0 * * *"`
	RollupRecentSchedule  string `env:"ROLLUP_RECENT_SCHEDULE" envDefault:"0 */5 * * * *"`
	ExpireOrdersSchedule  string `env:"EXPIRE_ORDERS_SCHEDULE" envDefault:"30 * * * * *"`

	SessionPurgeBatchSize int           `env:"SESSION_PURGE_BATCH_SIZE" envDefault:"1000"`
	PendingOrderTTL       time.Duration `env:"PENDING_ORDER_TTL" envDefault:"30m"`
	RollupRecentWindow    time.Duration `env:"ROLLUP_RECENT_WINDOW" envDefault:"5m"`
}

func parseConfig(getenv func(string) string) (config, error) {
	var cfg config
	params, err := env.GetFieldParams(&cfg)
	if err != nil {
		return config{}, fmt.Errorf("inspect config fields: %w", err)
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
	case cfg.SessionPurgeBatchSize < 1:
		return config{}, fmt.Errorf("%w: SESSION_PURGE_BATCH_SIZE must be positive", errInvalidConfig)
	case cfg.PendingOrderTTL <= 0:
		return config{}, fmt.Errorf("%w: PENDING_ORDER_TTL must be positive", errInvalidConfig)
	case cfg.ShutdownTimeout <= 0:
		return config{}, fmt.Errorf("%w: SHUTDOWN_TIMEOUT must be positive", errInvalidConfig)
	case cfg.RollupRecentWindow < 0 || (cfg.RollupRecentWindow > 0 && (24*time.Hour)%cfg.RollupRecentWindow != 0):
		return config{}, fmt.Errorf("%w: ROLLUP_RECENT_WINDOW must divide 24h, or be 0 to disable the job", errInvalidConfig)
	}
	return cfg, nil
}
