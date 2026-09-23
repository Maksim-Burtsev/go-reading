package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/backoff"
)

const maxDrainBytes = 64 << 10

// Config tunes the concurrency and retry behavior of a Pool.
type Config struct {
	Workers     int
	MaxAttempts int
	// MaxPerHost caps the attempts in flight to one target host. Zero means
	// no limit.
	MaxPerHost     int
	AttemptTimeout time.Duration
	Backoff        backoff.Policy
}

// Pool delivers tasks with bounded concurrency, retrying transient failures.
type Pool struct {
	client  *http.Client
	cfg     Config
	logger  *slog.Logger
	hosts   *hostLimiter
	results chan Result
}

// NewPool returns a Pool that sends deliveries with client.
func NewPool(client *http.Client, cfg Config, logger *slog.Logger) *Pool {
	return &Pool{
		client:  client,
		cfg:     cfg,
		logger:  logger,
		hosts:   newHostLimiter(cfg.MaxPerHost),
		results: make(chan Result, cfg.Workers),
	}
}

// Results returns the channel on which Run emits exactly one Result per task.
// The caller must keep receiving from it until it is closed, which happens
// when Run returns.
func (p *Pool) Results() <-chan Result {
	return p.results
}

// Run delivers tasks, at most Config.Workers at a time, until tasks is closed
// and every delivery has finished. Canceling ctx aborts in-flight deliveries
// and fails the remaining tasks without attempting them; Run then returns the
// context's error. Run must be called at most once.
func (p *Pool) Run(ctx context.Context, tasks <-chan Task) error {
	defer close(p.results)

	var g errgroup.Group
	g.SetLimit(p.cfg.Workers)
	for task := range tasks {
		g.Go(func() error {
			res := p.deliver(ctx, task)
			p.logResult(ctx, res)
			p.results <- res
			if res.Status == StatusCanceled {
				return res.Err
			}
			return nil
		})
	}
	return g.Wait()
}

func (p *Pool) deliver(ctx context.Context, t Task) Result {
	if err := ctx.Err(); err != nil {
		return Result{TaskID: t.ID, Status: StatusCanceled, Err: err}
	}
	host := hostOf(t.URL)

	var err error
	for attempt := range p.cfg.MaxAttempts {
		if attempt > 0 {
			delay := p.cfg.Backoff.Delay(attempt - 1)
			p.logger.WarnContext(ctx, "delivery attempt failed",
				"id", t.ID, "attempt", attempt, "retry_in", delay.String(), "error", err)
			if err := backoff.Sleep(ctx, delay); err != nil {
				return Result{TaskID: t.ID, Status: StatusCanceled, Attempts: attempt, Err: err}
			}
		}

		var release func()
		release, err = p.hosts.acquire(ctx, host)
		if err != nil {
			return Result{TaskID: t.ID, Status: StatusCanceled, Attempts: attempt, Err: err}
		}
		defer release()

		err = p.attempt(ctx, t)
		switch {
		case err == nil:
			return Result{TaskID: t.ID, Status: StatusDelivered, Attempts: attempt + 1}
		case ctx.Err() != nil:
			return Result{TaskID: t.ID, Status: StatusCanceled, Attempts: attempt + 1, Err: ctx.Err()}
		case errors.Is(err, ErrPermanent):
			return Result{TaskID: t.ID, Status: StatusFailed, Attempts: attempt + 1, Err: err}
		}
	}
	return Result{
		TaskID:   t.ID,
		Status:   StatusFailed,
		Attempts: p.cfg.MaxAttempts,
		Err:      fmt.Errorf("%w after %d attempts: %w", ErrRetriesExhausted, p.cfg.MaxAttempts, err),
	}
}

func (p *Pool) attempt(ctx context.Context, t Task) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, bytes.NewReader(t.Payload))
	if err != nil {
		return fmt.Errorf("%w: build request: %w", ErrPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", t.ID)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))

	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusTooManyRequests || code >= http.StatusInternalServerError:
		return &StatusError{Code: code}
	default:
		return fmt.Errorf("%w: %w", ErrPermanent, &StatusError{Code: code})
	}
}

func (p *Pool) logResult(ctx context.Context, res Result) {
	if res.Err != nil {
		p.logger.ErrorContext(ctx, "delivery failed",
			"id", res.TaskID, "status", res.Status, "attempts", res.Attempts, "error", res.Err)
		return
	}
	p.logger.InfoContext(ctx, "delivery succeeded", "id", res.TaskID, "attempts", res.Attempts)
}
