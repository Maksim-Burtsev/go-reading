// Package batcher buffers events in memory and writes them to storage in batches.
package batcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
)

const maxRetryBackoff = 10 * time.Second

var (
	// ErrBufferFull is returned by Enqueue when the buffer has no room for all events.
	ErrBufferFull = errors.New("buffer full")
	// ErrTooLarge is returned by Enqueue when the events can never fit into the buffer.
	ErrTooLarge = errors.New("request exceeds buffer capacity")
	// ErrClosed is returned by Enqueue once the batcher has started draining.
	ErrClosed = errors.New("batcher closed")
)

// Inserter persists a batch of events.
type Inserter interface {
	InsertEvents(ctx context.Context, events []event.Event) error
}

// Config controls buffering, batching and retry behaviour.
type Config struct {
	BufferSize    int
	MaxBytes      int64
	BatchSize     int
	FlushInterval time.Duration
	MaxAttempts   int
	RetryBackoff  time.Duration
	DrainTimeout  time.Duration
}

// Batcher accumulates events in a bounded buffer and flushes them with a
// single goroutine started by Run.
type Batcher struct {
	cfg    Config
	ins    Inserter
	logger *slog.Logger

	mu     sync.Mutex
	closed bool
	events chan event.Event
	bytes  atomic.Int64

	batchSize     prometheus.Histogram
	batchBytes    prometheus.Histogram
	flushDuration prometheus.Histogram
	flushErrors   prometheus.Counter
	dropped       prometheus.Counter
}

// New returns a Batcher and registers its metrics with reg.
func New(cfg Config, ins Inserter, reg prometheus.Registerer, logger *slog.Logger) (*Batcher, error) {
	b := &Batcher{
		cfg:    cfg,
		ins:    ins,
		logger: logger,
		events: make(chan event.Event, cfg.BufferSize),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "sink",
			Name:      "batch_size_events",
			Help:      "Number of events per batch, stored or dropped.",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 8),
		}),
		batchBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "sink",
			Name:      "batch_size_bytes",
			Help:      "Payload bytes per batch, stored or dropped.",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 8),
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "sink",
			Name:      "flush_duration_seconds",
			Help:      "Time spent flushing a batch, including retries.",
			Buckets:   prometheus.DefBuckets,
		}),
		flushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "sink",
			Name:      "flush_errors_total",
			Help:      "Failed insert attempts.",
		}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "sink",
			Name:      "events_dropped_total",
			Help:      "Events discarded after all insert attempts failed.",
		}),
	}
	bufferLength := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sink",
		Name:      "buffer_length",
		Help:      "Events waiting in the buffer.",
	}, func() float64 { return float64(b.Len()) })
	bufferBytes := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sink",
		Name:      "buffer_bytes",
		Help:      "Payload bytes held in memory, including the batch being flushed.",
	}, b.bufferedBytes)

	for _, c := range []prometheus.Collector{b.batchSize, b.batchBytes, b.flushDuration, b.flushErrors, b.dropped, bufferLength, bufferBytes} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register batcher metric: %w", err)
		}
	}
	return b, nil
}

// Len returns the number of buffered events.
func (b *Batcher) Len() int {
	return len(b.events)
}

func (b *Batcher) bufferedBytes() float64 {
	return float64(b.bytes.Load())
}

// Enqueue adds all events to the buffer or none of them. It never blocks.
func (b *Batcher) Enqueue(events []event.Event) error {
	size := payloadBytes(events)
	if len(events) > cap(b.events) || size > b.cfg.MaxBytes {
		return ErrTooLarge
	}
	if b.bytes.Load()+size > b.cfg.MaxBytes {
		return ErrBufferFull
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}
	if cap(b.events)-len(b.events) < len(events) {
		return ErrBufferFull
	}
	b.bytes.Add(size)
	for _, e := range events {
		b.events <- e
	}
	return nil
}

// Run flushes buffered events whenever a batch fills up or the flush interval
// elapses. When ctx is cancelled it stops accepting events, flushes what is
// left and returns; the drain timeout, counted from the cancellation, bounds
// the flush in progress and the drain together.
func (b *Batcher) Run(ctx context.Context) error {
	flushCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(b.cfg.DrainTimeout, cancel) })
	defer stop()

	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]event.Event, 0, b.cfg.BatchSize)
	for {
		select {
		case <-ctx.Done():
			return b.drain(flushCtx, batch)
		case e := <-b.events:
			batch = append(batch, e)
			if len(batch) < b.cfg.BatchSize {
				continue
			}
			_ = b.flush(flushCtx, batch)
			batch = make([]event.Event, 0, b.cfg.BatchSize)
			ticker.Reset(b.cfg.FlushInterval)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}
			_ = b.flush(flushCtx, batch)
			batch = make([]event.Event, 0, b.cfg.BatchSize)
		}
	}
}

func (b *Batcher) drain(ctx context.Context, batch []event.Event) error {
	b.mu.Lock()
	b.closed = true
	close(b.events)
	b.mu.Unlock()

	b.logger.InfoContext(ctx, "draining buffer", "events", len(batch)+len(b.events))

	var errs []error
	for e := range b.events {
		batch = append(batch, e)
		if len(batch) == b.cfg.BatchSize {
			errs = append(errs, b.flush(ctx, batch))
			batch = make([]event.Event, 0, b.cfg.BatchSize)
		}
	}
	if len(batch) > 0 {
		errs = append(errs, b.flush(ctx, batch))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}

func (b *Batcher) flush(ctx context.Context, batch []event.Event) error {
	start := time.Now()
	err := b.insertWithRetry(ctx, batch)
	b.flushDuration.Observe(time.Since(start).Seconds())
	b.batchSize.Observe(float64(len(batch)))

	size := payloadBytes(batch)
	b.batchBytes.Observe(float64(size))
	if err != nil {
		b.dropped.Add(float64(len(batch)))
		b.logger.ErrorContext(ctx, "batch dropped", "events", len(batch), "error", err)
		return err
	}
	b.bytes.Add(-size)
	return nil
}

// payloadBytes approximates the memory the events hold by the length of their
// variable-size fields.
func payloadBytes(events []event.Event) int64 {
	var n int64
	for i := range events {
		n += int64(len(events[i].Type) + len(events[i].UserID) + len(events[i].Properties))
	}
	return n
}

func (b *Batcher) insertWithRetry(ctx context.Context, batch []event.Event) error {
	delay := b.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := b.ins.InsertEvents(ctx, batch)
		if err == nil {
			return nil
		}
		b.flushErrors.Inc()
		if attempt >= b.cfg.MaxAttempts {
			return fmt.Errorf("insert failed after %d attempts: %w", attempt, err)
		}
		b.logger.WarnContext(ctx, "insert failed, retrying", "attempt", attempt, "delay", delay.String(), "error", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("insert retry aborted: %w", errors.Join(err, ctx.Err()))
		case <-time.After(delay):
		}
		delay = min(delay*2, maxRetryBackoff)
	}
}
