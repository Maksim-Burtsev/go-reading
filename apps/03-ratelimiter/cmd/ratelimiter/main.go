// Command ratelimiter runs a demo HTTP API rate limited per client IP with the ratelimit package.
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

	"github.com/Maksim-Burtsev/go-reading/apps/03-ratelimiter/internal/config"
	"github.com/Maksim-Burtsev/go-reading/apps/03-ratelimiter/internal/server"
	"github.com/Maksim-Burtsev/go-reading/apps/03-ratelimiter/ratelimit"
)

var errUsage = errors.New("ratelimiter takes no arguments, it is configured through the environment")

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "ratelimiter: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, _ io.Writer) error {
	if len(args) > 1 {
		return fmt.Errorf("%w: got %q", errUsage, args[1:])
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	limiter, err := newLimiter(cfg)
	if err != nil {
		return fmt.Errorf("create limiter: %w", err)
	}
	defer limiter.Close()

	var keyFunc ratelimit.KeyFunc = ratelimit.RemoteIP
	if len(cfg.TrustedProxies) > 0 {
		keyFunc = ratelimit.ForwardedFor(cfg.TrustedProxies)
	}

	srv := &http.Server{
		Handler:           server.New(logger, ratelimit.Middleware(limiter, keyFunc, logger)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.InfoContext(ctx, "listening", slog.String("addr", ln.Addr().String()), slog.String("algorithm", string(cfg.Algorithm)))

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

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
	logger.InfoContext(ctx, "stopped")
	return nil
}

type limiter interface {
	ratelimit.Limiter
	Close()
}

func newLimiter(cfg config.Config) (limiter, error) {
	rate := ratelimit.Rate{Limit: cfg.Limit, Period: cfg.Period}
	cleanup := ratelimit.WithCleanupInterval(cfg.CleanupInterval)
	switch cfg.Algorithm {
	case config.TokenBucket:
		return ratelimit.NewTokenBucket(rate, cfg.Burst, cleanup)
	case config.SlidingWindow:
		return ratelimit.NewSlidingWindow(rate, cleanup)
	default:
		return nil, fmt.Errorf("%w: %q", config.ErrUnknownAlgorithm, cfg.Algorithm)
	}
}
