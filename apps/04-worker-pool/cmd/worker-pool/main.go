// Command worker-pool is a webhook dispatcher: it accepts deliveries over HTTP,
// queues them in memory and POSTs them to their targets with retries.
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

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/api"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/backoff"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/status"
)

type config struct {
	Addr            string        `env:"ADDR" envDefault:":8080"`
	Workers         int           `env:"WORKERS" envDefault:"8"`
	QueueSize       int           `env:"QUEUE_SIZE" envDefault:"1024"`
	MaxAttempts     int           `env:"MAX_ATTEMPTS" envDefault:"5"`
	BackoffBase     time.Duration `env:"BACKOFF_BASE" envDefault:"500ms"`
	BackoffMax      time.Duration `env:"BACKOFF_MAX" envDefault:"30s"`
	AttemptTimeout  time.Duration `env:"ATTEMPT_TIMEOUT" envDefault:"10s"`
	DrainTimeout    time.Duration `env:"DRAIN_TIMEOUT" envDefault:"20s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"5s"`
	ResultRetention time.Duration `env:"RESULT_RETENTION" envDefault:"1h"`
	MaxBodyBytes    int64         `env:"MAX_BODY_BYTES" envDefault:"1048576"`
}

func (c config) validate() error {
	var errs []error
	if c.Workers < 1 {
		errs = append(errs, fmt.Errorf("WORKERS must be at least 1, got %d", c.Workers))
	}
	if c.QueueSize < 1 {
		errs = append(errs, fmt.Errorf("QUEUE_SIZE must be at least 1, got %d", c.QueueSize))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("MAX_ATTEMPTS must be at least 1, got %d", c.MaxAttempts))
	}
	if c.BackoffBase <= 0 || c.BackoffMax < c.BackoffBase {
		errs = append(errs, fmt.Errorf("need 0 < BACKOFF_BASE <= BACKOFF_MAX, got %s and %s", c.BackoffBase, c.BackoffMax))
	}
	if c.AttemptTimeout <= 0 {
		errs = append(errs, fmt.Errorf("ATTEMPT_TIMEOUT must be positive, got %s", c.AttemptTimeout))
	}
	if c.ResultRetention <= 0 {
		errs = append(errs, fmt.Errorf("RESULT_RETENTION must be positive, got %s", c.ResultRetention))
	}
	if c.MaxBodyBytes < 1 {
		errs = append(errs, fmt.Errorf("MAX_BODY_BYTES must be at least 1, got %d", c.MaxBodyBytes))
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
		return config{}, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return config{}, fmt.Errorf("validate: %w", err)
	}
	return cfg, nil
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
		return fmt.Errorf("load config: %w", err)
	}

	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	queue := dispatch.NewQueue(cfg.QueueSize)
	recorder := status.NewRecorder(cfg.ResultRetention)
	pool := dispatch.NewPool(newClient(), dispatch.Config{
		Workers:        cfg.Workers,
		MaxAttempts:    cfg.MaxAttempts,
		AttemptTimeout: cfg.AttemptTimeout,
		Backoff:        backoff.Policy{Base: cfg.BackoffBase, Max: cfg.BackoffMax},
	}, logger)

	workCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	poolDone := make(chan error, 1)
	go func() {
		poolDone <- pool.Run(workCtx, queue.Tasks())
	}()
	recorderDone := make(chan struct{})
	go func() {
		defer close(recorderDone)
		recorder.Run(pool.Results())
	}()

	srv := &http.Server{
		Handler:           api.NewHandler(queue, recorder, logger, cfg.MaxBodyBytes),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	logger.InfoContext(ctx, "listening", "addr", ln.Addr().String(), "workers", cfg.Workers, "queue_size", cfg.QueueSize)
	serveErr := serve(ctx, srv, ln, cfg.ShutdownTimeout)

	queue.Close()
	logger.InfoContext(ctx, "draining queue", "pending", queue.Len(), "timeout", cfg.DrainTimeout.String())
	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), cfg.DrainTimeout)
	defer cancelDrain()
	context.AfterFunc(drainCtx, abort)
	if err := <-poolDone; err != nil {
		logger.WarnContext(ctx, "drain timeout exceeded, in-flight deliveries canceled", "error", err)
	}
	<-recorderDone
	logger.InfoContext(ctx, "stopped")
	return serveErr
}

func newClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	var err error
	select {
	case <-ctx.Done():
	case err = <-serveErr:
		err = fmt.Errorf("serve: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if serr := srv.Shutdown(shutdownCtx); serr != nil {
		err = errors.Join(err, fmt.Errorf("shutdown http server: %w", serr))
	}
	return err
}
