// Package pipeline moves events from a Kafka consumer group to ClickHouse in
// batches and commits offsets only after a batch is stored and recorded.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/ledger"
)

// Headers added to every record produced to the dead-letter topic.
const (
	HeaderTopic     = "dlq-original-topic"
	HeaderPartition = "dlq-original-partition"
	HeaderOffset    = "dlq-original-offset"
	HeaderError     = "dlq-error"
)

const maxBackoff = 10 * time.Second

var (
	errExhausted = errors.New("retry attempts exhausted")
	errCommit    = errors.New("offset commit failed")
)

// Client is the part of *kgo.Client the Pipeline uses.
type Client interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// Inserter stores events. A batch is inserted again after a failed attempt or
// a restart, and in parts after the whole of it was refused, so the store must
// collapse duplicate events.
type Inserter interface {
	InsertEvents(ctx context.Context, events []event.Event) error
}

// Recorder keeps the log of flushed batches.
type Recorder interface {
	Record(ctx context.Context, b ledger.Batch) error
}

// Config tunes batching, retries, dead-lettering and shutdown. AttemptTimeout
// bounds every single call to ClickHouse, Postgres or Kafka a flush makes.
type Config struct {
	DeadLetterTopic string
	BatchSize       int
	BatchTimeout    time.Duration
	MaxAttempts     int
	RetryBackoff    time.Duration
	AttemptTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// Pipeline consumes records on a single goroutine with at-least-once
// delivery. Records that are not valid events, and events the Inserter keeps
// rejecting, go to the dead-letter topic.
type Pipeline struct {
	client   Client
	events   Inserter
	ledger   Recorder
	cfg      Config
	logger   *slog.Logger
	metrics  metrics
	pending  []pending
	deadline time.Time
}

type pending struct {
	record *kgo.Record
	event  event.Event
	err    error
}

type rejection struct {
	index int
	err   error
}

type metrics struct {
	consumed      prometheus.Counter
	flushed       *prometheus.CounterVec
	deadLettered  *prometheus.CounterVec
	rejected      prometheus.Gauge
	batchSize     prometheus.Histogram
	flushDuration prometheus.Histogram
}

// ClientOptions returns the kgo options the Pipeline relies on: manual commits,
// and rebalances held back while a polled batch is uncommitted.
func ClientOptions(group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
}

// New returns a Pipeline and registers its metrics with reg. The client must
// be created with ClientOptions.
func New(client Client, events Inserter, ledger Recorder, cfg Config, reg prometheus.Registerer, logger *slog.Logger) (*Pipeline, error) {
	const ns = "eventsink"
	m := metrics{
		consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "records_consumed_total", Help: "Records polled from Kafka.",
		}),
		flushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "batches_flushed_total", Help: "Flushed batches, by outcome.",
		}, []string{"outcome"}),
		deadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "dead_letters_total", Help: "Records produced to the dead-letter topic, by reason.",
		}, []string{"reason"}),
		rejected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "rejected_events", Help: "Events the Inserter refused even on their own.",
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "batch_size_records", Help: "Records per flushed batch.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8),
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "flush_duration_seconds", Help: "Time from the start of a flush to the offset commit.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	for _, c := range []prometheus.Collector{m.consumed, m.flushed, m.deadLettered, m.rejected, m.batchSize, m.flushDuration} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register pipeline metric: %w", err)
		}
	}
	return &Pipeline{client: client, events: events, ledger: ledger, cfg: cfg, logger: logger, metrics: m}, nil
}

// Run consumes until ctx is done, then flushes and commits the pending batch.
// A flush in progress, and the final one, may outlive ctx by
// Config.ShutdownTimeout. An error means some records were left uncommitted
// and will be delivered again. A failed commit before shutdown is only logged:
// its records are read again after a restart or rebalance unless a later
// commit covers their partitions.
func (p *Pipeline) Run(ctx context.Context) error {
	flushCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(p.cfg.ShutdownTimeout, cancel) })
	defer stop()

	for ctx.Err() == nil {
		if err := p.poll(ctx); err != nil {
			return err
		}
		if len(p.pending) >= p.cfg.BatchSize || len(p.pending) > 0 && !time.Now().Before(p.deadline) {
			switch err := p.flush(flushCtx); {
			case errors.Is(err, errCommit) && ctx.Err() == nil:
				p.logger.ErrorContext(ctx, "commit failed", "error", err)
			case err != nil:
				return err
			}
		}
		if len(p.pending) == 0 {
			p.client.AllowRebalance()
		}
	}

	p.logger.InfoContext(flushCtx, "flushing pending batch", "records", len(p.pending))
	if err := p.flush(flushCtx); err != nil {
		return err
	}
	p.client.AllowRebalance()
	return nil
}

func (p *Pipeline) poll(ctx context.Context) error {
	pollCtx, cancel := p.pollContext(ctx)
	defer cancel()

	fetches := p.client.PollRecords(pollCtx, p.cfg.BatchSize-len(p.pending))
	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
			continue
		}
		if errors.Is(fe.Err, kgo.ErrClientClosed) {
			return fmt.Errorf("poll: %w", fe.Err)
		}
		p.logger.ErrorContext(ctx, "fetch failed", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
	}

	now := time.Now()
	fetches.EachRecord(func(r *kgo.Record) {
		if len(p.pending) == 0 {
			p.deadline = now.Add(p.cfg.BatchTimeout)
		}
		e, err := event.Decode(r.Value)
		p.pending = append(p.pending, pending{record: r, event: e, err: err})
		p.metrics.consumed.Inc()
	})
	return nil
}

func (p *Pipeline) pollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if len(p.pending) > 0 {
		return context.WithDeadline(ctx, p.deadline)
	}
	return context.WithCancel(ctx)
}

func (p *Pipeline) flush(ctx context.Context) error {
	items := p.pending
	if len(items) == 0 {
		return nil
	}
	p.pending = nil
	start := time.Now()

	records := make([]*kgo.Record, 0, len(items))
	var (
		events []event.Event
		valid  []*kgo.Record
		dead   []*kgo.Record
	)
	for _, it := range items {
		records = append(records, it.record)
		if it.err != nil {
			dead = append(dead, p.deadLetter(it.record, it.err))
			continue
		}
		events = append(events, it.event)
		valid = append(valid, it.record)
	}
	invalid := len(dead)

	batch := ledger.Batch{Status: ledger.StatusInserted, Records: len(records), Partitions: partitions(records)}
	rejected, err := p.insert(ctx, events)
	batch.InsertDuration = time.Since(start)
	if err != nil {
		return fmt.Errorf("insert %d events: %w", len(events), err)
	}
	p.metrics.rejected.Set(float64(len(rejected)))
	if len(rejected) > 0 {
		p.logger.ErrorContext(ctx, "dead-lettering rejected events", "events", len(rejected))
		batch.Status = ledger.StatusPartiallyDeadLettered
		if len(rejected) == len(events) {
			batch.Status = ledger.StatusDeadLettered
		}
		for i := range rejected {
			dead = append(dead, p.deadLetter(valid[i], rejected[i].err))
		}
	}

	batch.DeadLettered = len(dead)
	if len(dead) > 0 {
		if err := p.attempt(ctx, func(ctx context.Context) error {
			return p.client.ProduceSync(ctx, dead...).FirstErr()
		}); err != nil {
			return fmt.Errorf("produce %d dead letters: %w", len(dead), err)
		}
		p.metrics.deadLettered.WithLabelValues("invalid").Add(float64(invalid))
		p.metrics.deadLettered.WithLabelValues("insert_failed").Add(float64(len(dead) - invalid))
	}

	if err := p.retry(ctx, "record batch", func(ctx context.Context) error {
		return p.ledger.Record(ctx, batch)
	}); err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	commitErr := p.attempt(ctx, func(ctx context.Context) error {
		return p.client.CommitRecords(ctx, records...)
	})

	p.metrics.flushed.WithLabelValues(string(batch.Status)).Inc()
	p.metrics.batchSize.Observe(float64(len(records)))
	p.metrics.flushDuration.Observe(time.Since(start).Seconds())
	p.logger.InfoContext(ctx, "batch flushed", "status", batch.Status, "records", batch.Records,
		"dead_lettered", batch.DeadLettered, "insert_duration", batch.InsertDuration.String())
	if commitErr != nil {
		return fmt.Errorf("%w for %d records: %w", errCommit, len(records), commitErr)
	}
	return nil
}

// insert stores events. When an insert runs out of attempts, the events are
// split in halves and each half is inserted the same way, down to single
// events, so a few events ClickHouse refuses do not take the rest of the batch
// to the dead-letter topic. It returns the events that were refused on their
// own, with the error of their last attempt.
func (p *Pipeline) insert(ctx context.Context, events []event.Event) ([]rejection, error) {
	if len(events) == 0 {
		return nil, nil
	}
	err := p.retry(ctx, "insert events", func(ctx context.Context) error {
		return p.events.InsertEvents(ctx, events)
	})
	switch {
	case err == nil:
		return nil, nil
	case !errors.Is(err, errExhausted) || ctx.Err() != nil:
		return nil, err
	case len(events) == 1:
		return []rejection{{index: 0, err: err}}, nil
	}

	mid := len(events) / 2
	left, err := p.insert(ctx, events[:mid])
	if err != nil {
		return nil, err
	}
	right, err := p.insert(ctx, events[mid:])
	if err != nil {
		return nil, err
	}
	for i := range right {
		right[i].index += mid
	}
	return append(left, right...), nil
}

func (p *Pipeline) retry(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	delay := p.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := p.attempt(ctx, fn)
		if err == nil {
			return nil
		}
		p.logger.ErrorContext(ctx, op+" failed", "attempt", attempt, "error", err)
		if attempt >= p.cfg.MaxAttempts {
			return fmt.Errorf("%w after %d attempts: %w", errExhausted, attempt, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait before attempt %d: %w (last error: %w)", attempt+1, ctx.Err(), err)
		case <-time.After(delay):
		}
		delay = min(delay*2, maxBackoff)
	}
}

// attempt calls fn with a context that expires after Config.AttemptTimeout, so
// no single call can hold a batch, and the rebalances waiting for it, for long.
func (p *Pipeline) attempt(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	return fn(ctx)
}

func (p *Pipeline) deadLetter(r *kgo.Record, cause error) *kgo.Record {
	return &kgo.Record{
		Topic: p.cfg.DeadLetterTopic,
		Key:   r.Key,
		Value: r.Value,
		Headers: append(slices.Clone(r.Headers),
			kgo.RecordHeader{Key: HeaderTopic, Value: []byte(r.Topic)},
			kgo.RecordHeader{Key: HeaderPartition, Value: strconv.AppendInt(nil, int64(r.Partition), 10)},
			kgo.RecordHeader{Key: HeaderOffset, Value: strconv.AppendInt(nil, r.Offset, 10)},
			kgo.RecordHeader{Key: HeaderError, Value: []byte(cause.Error())},
		),
	}
}

func partitions(records []*kgo.Record) []ledger.Partition {
	var out []ledger.Partition
	for _, r := range records {
		i := slices.IndexFunc(out, func(p ledger.Partition) bool { return p.Topic == r.Topic && p.Partition == r.Partition })
		if i < 0 {
			i = len(out)
			out = append(out, ledger.Partition{Topic: r.Topic, Partition: r.Partition, FirstOffset: r.Offset, LastOffset: r.Offset})
		}
		out[i].FirstOffset = min(out[i].FirstOffset, r.Offset)
		out[i].LastOffset = max(out[i].LastOffset, r.Offset)
		out[i].Records++
	}
	return out
}
