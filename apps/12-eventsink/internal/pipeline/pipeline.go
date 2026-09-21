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

var errExhausted = errors.New("retry attempts exhausted")

// Client is the part of *kgo.Client the Pipeline uses.
type Client interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// Inserter stores events. A batch is inserted again after a failed attempt or
// a restart, so the store must collapse duplicate events.
type Inserter interface {
	InsertEvents(ctx context.Context, events []event.Event) error
}

// Recorder keeps the log of flushed batches.
type Recorder interface {
	Record(ctx context.Context, b ledger.Batch) error
}

// Config tunes batching, retries, dead-lettering and shutdown.
type Config struct {
	DeadLetterTopic string
	BatchSize       int
	BatchTimeout    time.Duration
	MaxAttempts     int
	RetryBackoff    time.Duration
	ShutdownTimeout time.Duration
}

// Pipeline consumes records on a single goroutine with at-least-once
// delivery. Records that are not valid events, and batches the Inserter keeps
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

type metrics struct {
	consumed      prometheus.Counter
	flushed       *prometheus.CounterVec
	deadLettered  *prometheus.CounterVec
	batchSize     prometheus.Histogram
	flushDuration prometheus.Histogram
}

// ClientOptions returns the kgo options the Pipeline relies on: manual commits,
// rebalances held back while a polled batch is uncommitted, and topics created
// on first use where the brokers allow it.
func ClientOptions(group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
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
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "batch_size_records", Help: "Records per flushed batch.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8),
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "flush_duration_seconds", Help: "Time from the start of a flush to the offset commit.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	for _, c := range []prometheus.Collector{m.consumed, m.flushed, m.deadLettered, m.batchSize, m.flushDuration} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register pipeline metric: %w", err)
		}
	}
	return &Pipeline{client: client, events: events, ledger: ledger, cfg: cfg, logger: logger, metrics: m}, nil
}

// Run consumes until ctx is done, then flushes and commits the pending batch.
// A flush in progress, and the final one, may outlive ctx by
// Config.ShutdownTimeout. An error means some records were left uncommitted
// and will be delivered again.
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
			if err := p.flush(flushCtx); err != nil {
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
		dead   []*kgo.Record
	)
	for _, it := range items {
		records = append(records, it.record)
		if it.err != nil {
			dead = append(dead, p.deadLetter(it.record, it.err))
			continue
		}
		events = append(events, it.event)
	}
	invalid := len(dead)

	batch := ledger.Batch{Status: ledger.StatusInserted, Records: len(records), Partitions: partitions(records)}
	err := p.retry(ctx, "insert events", func(ctx context.Context) error {
		return p.events.InsertEvents(ctx, events)
	})
	batch.InsertDuration = time.Since(start)
	if err != nil {
		if !errors.Is(err, errExhausted) || ctx.Err() != nil {
			return fmt.Errorf("insert %d events: %w", len(events), err)
		}
		p.logger.ErrorContext(ctx, "dead-lettering batch", "events", len(events), "error", err)
		batch.Status = ledger.StatusDeadLettered
		for _, it := range items {
			if it.err == nil {
				dead = append(dead, p.deadLetter(it.record, err))
			}
		}
	}

	batch.DeadLettered = len(dead)
	if len(dead) > 0 {
		if err := p.client.ProduceSync(ctx, dead...).FirstErr(); err != nil {
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
	if err := p.client.CommitRecords(ctx, records...); err != nil {
		p.logger.ErrorContext(ctx, "commit failed", "records", len(records), "error", err)
	}

	p.metrics.flushed.WithLabelValues(string(batch.Status)).Inc()
	p.metrics.batchSize.Observe(float64(len(records)))
	p.metrics.flushDuration.Observe(time.Since(start).Seconds())
	p.logger.InfoContext(ctx, "batch flushed", "status", batch.Status, "records", batch.Records,
		"dead_lettered", batch.DeadLettered, "insert_duration", batch.InsertDuration.String())
	return nil
}

func (p *Pipeline) retry(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	delay := p.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
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
