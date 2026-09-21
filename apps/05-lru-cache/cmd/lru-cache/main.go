// Command lru-cache is a read-through HTTP caching proxy backed by an
// in-memory LRU cache.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"golang.org/x/sync/errgroup"

	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/internal/proxy"
	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/lru"
)

var errInvalidConfig = errors.New("invalid config")

type config struct {
	Addr            string        `env:"ADDR"             envDefault:"localhost:8080"`
	UpstreamURL     url.URL       `env:"UPSTREAM_URL"     envDefault:"https://proxy.golang.org"`
	UpstreamTimeout time.Duration `env:"UPSTREAM_TIMEOUT" envDefault:"10s"`
	MaxBodyBytes    int64         `env:"MAX_BODY_BYTES"   envDefault:"1048576"`
	CacheSize       int           `env:"CACHE_SIZE"       envDefault:"1024"`
	CacheTTL        time.Duration `env:"CACHE_TTL"        envDefault:"5m"`
	JanitorInterval time.Duration `env:"JANITOR_INTERVAL" envDefault:"1m"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        slog.Level    `env:"LOG_LEVEL"        envDefault:"INFO"`
}

func (c config) validate() error {
	switch {
	case c.UpstreamTimeout <= 0:
		return fmt.Errorf("%w: UPSTREAM_TIMEOUT must be positive", errInvalidConfig)
	case c.MaxBodyBytes <= 0:
		return fmt.Errorf("%w: MAX_BODY_BYTES must be positive", errInvalidConfig)
	case c.CacheSize <= 0:
		return fmt.Errorf("%w: CACHE_SIZE must be positive", errInvalidConfig)
	case c.CacheTTL < 0:
		return fmt.Errorf("%w: CACHE_TTL must not be negative", errInvalidConfig)
	case c.JanitorInterval <= 0:
		return fmt.Errorf("%w: JANITOR_INTERVAL must be positive", errInvalidConfig)
	case c.ShutdownTimeout <= 0:
		return fmt.Errorf("%w: SHUTDOWN_TIMEOUT must be positive", errInvalidConfig)
	}
	return nil
}

func loadConfig(getenv func(string) string) (config, error) {
	params, err := env.GetFieldParams(&config{})
	if err != nil {
		return config{}, fmt.Errorf("inspect config: %w", err)
	}
	environment := make(map[string]string, len(params))
	for _, p := range params {
		if v := getenv(p.Key); v != "" {
			environment[p.Key] = v
		}
	}
	cfg, err := env.ParseAsWithOptions[config](env.Options{Environment: environment})
	if err != nil {
		return config{}, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
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

	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	cache, err := lru.New(cfg.CacheSize,
		lru.WithTTL[string, *proxy.Response](cfg.CacheTTL),
		lru.WithOnEvict(func(key string, _ *proxy.Response, reason lru.EvictReason) {
			logger.DebugContext(ctx, "cache eviction", "key", key, "reason", reason.String())
		}),
	)
	if err != nil {
		return fmt.Errorf("create cache: %w", err)
	}
	p, err := proxy.New(&cfg.UpstreamURL, &http.Client{Timeout: cfg.UpstreamTimeout}, cache, cfg.MaxBodyBytes, logger)
	if err != nil {
		return fmt.Errorf("create proxy: %w", err)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           p.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.UpstreamTimeout + 10*time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	logger.InfoContext(ctx, "listening", "addr", ln.Addr().String(), "upstream", cfg.UpstreamURL.Redacted())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		logger.InfoContext(gctx, "shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	})
	if cfg.CacheTTL > 0 {
		g.Go(func() error {
			sweep(gctx, logger, cache, cfg.JanitorInterval)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	logger.InfoContext(ctx, "stopped")
	return nil
}

type expirer interface {
	DeleteExpired() int
}

func sweep(ctx context.Context, logger *slog.Logger, cache expirer, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := cache.DeleteExpired(); n > 0 {
				logger.InfoContext(ctx, "expired entries removed", "count", n)
			}
		}
	}
}
