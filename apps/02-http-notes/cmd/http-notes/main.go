// Command http-notes serves an in-memory notes store over a JSON HTTP API.
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

	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/httpapi"
	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/notes"
)

type config struct {
	Addr              string        `env:"ADDR" envDefault:":8080"`
	LogLevel          slog.Level    `env:"LOG_LEVEL" envDefault:"INFO"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT" envDefault:"15s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`
	HandlerTimeout    time.Duration `env:"HANDLER_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout   time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
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

	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}

	logger := slog.New(httpapi.NewLogHandler(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))

	srv := &http.Server{
		Handler:           httpapi.NewHandler(logger, notes.NewStore(), cfg.HandlerTimeout),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	logger.InfoContext(ctx, "http server started", slog.String("addr", ln.Addr().String()))

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve http: %w", err)
	case <-ctx.Done():
	}

	logger.InfoContext(ctx, "shutting down http server", slog.Duration("timeout", cfg.ShutdownTimeout))
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve http: %w", err)
	}
	logger.InfoContext(ctx, "http server stopped")
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

	if cfg.HandlerTimeout >= cfg.WriteTimeout {
		return config{}, fmt.Errorf("handler timeout %s must be shorter than write timeout %s", cfg.HandlerTimeout, cfg.WriteTimeout)
	}
	return cfg, nil
}
