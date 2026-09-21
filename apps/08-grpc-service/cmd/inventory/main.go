// Command inventory serves the inventory gRPC API from an in-memory store.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/inventory"
	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/server"
)

type config struct {
	Addr            string        `env:"INVENTORY_ADDR" envDefault:":50051"`
	DefaultTimeout  time.Duration `env:"INVENTORY_DEFAULT_TIMEOUT" envDefault:"5s"`
	ShutdownTimeout time.Duration `env:"INVENTORY_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        slog.Level    `env:"INVENTORY_LOG_LEVEL" envDefault:"INFO"`
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
	logger := slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	store, err := inventory.NewStore(seedItems(), time.Now)
	if err != nil {
		return fmt.Errorf("seed inventory: %w", err)
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	srv := server.New(logger, store, cfg.DefaultTimeout) //nolint:contextcheck // stream interceptors take their context from grpc.ServerStream
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(lis)
	}()
	logger.InfoContext(ctx, "inventory server started", slog.String("addr", lis.Addr().String()))

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	logger.InfoContext(ctx, "shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; err != nil {
		return err
	}
	logger.InfoContext(ctx, "inventory server stopped")
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
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.DefaultTimeout <= 0 || cfg.ShutdownTimeout <= 0 {
		return config{}, errors.New("parse config: timeouts must be positive")
	}
	return cfg, nil
}

func seedItems() []inventory.Item {
	return []inventory.Item{
		{SKU: "BOOK-DDIA", Name: "Designing Data-Intensive Applications", Available: 5},
		{SKU: "BOOK-GOPL", Name: "The Go Programming Language", Available: 12},
		{SKU: "MUG-GOPHER", Name: "Gopher mug", Available: 40},
		{SKU: "STICKER-GOPHER", Name: "Gopher sticker pack", Available: 0},
		{SKU: "TSHIRT-GOPHER-M", Name: "Gopher T-shirt, size M", Available: 3},
	}
}
