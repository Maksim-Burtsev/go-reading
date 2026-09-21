// Package config loads the demo server configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"time"

	"github.com/caarlos0/env/v11"
)

// ErrUnknownAlgorithm is returned for an ALGORITHM value that names no algorithm.
var ErrUnknownAlgorithm = errors.New("unknown algorithm")

// Algorithm names a rate limiting algorithm.
type Algorithm string

// Supported algorithms.
const (
	TokenBucket   Algorithm = "token_bucket"
	SlidingWindow Algorithm = "sliding_window"
)

// UnmarshalText implements encoding.TextUnmarshaler.
func (a *Algorithm) UnmarshalText(text []byte) error {
	switch v := Algorithm(text); v {
	case TokenBucket, SlidingWindow:
		*a = v
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnknownAlgorithm, text)
	}
}

// Config is the demo server configuration.
type Config struct {
	Addr      string    `env:"ADDR" envDefault:"localhost:8080"`
	Algorithm Algorithm `env:"ALGORITHM" envDefault:"token_bucket"`
	// Limit requests are allowed per Period by either algorithm.
	Limit  int           `env:"LIMIT" envDefault:"5"`
	Period time.Duration `env:"PERIOD" envDefault:"1s"`
	// Burst is the bucket capacity of the token bucket. The sliding window ignores it.
	Burst           int           `env:"BURST" envDefault:"10"`
	CleanupInterval time.Duration `env:"CLEANUP_INTERVAL" envDefault:"1m"`
	// TrustedProxies are the comma-separated prefixes of reverse proxies whose
	// X-Forwarded-For header is trusted. When empty, clients are keyed by their peer address.
	TrustedProxies  []netip.Prefix `env:"TRUSTED_PROXIES"`
	ShutdownTimeout time.Duration  `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        slog.Level     `env:"LOG_LEVEL" envDefault:"INFO"`
}

// Load reads the configuration through getenv. Unset and empty variables take their defaults.
func Load(getenv func(string) string) (Config, error) {
	var cfg Config
	params, err := env.GetFieldParams(&cfg)
	if err != nil {
		return Config{}, fmt.Errorf("inspect config fields: %w", err)
	}

	environment := make(map[string]string, len(params))
	for _, p := range params {
		if v := getenv(p.Key); v != "" {
			environment[p.Key] = v
		}
	}
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environment}); err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}
	return cfg, nil
}
