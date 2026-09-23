// Package consumer reads events from a Kafka consumer group, writes them to a
// Sink in batches and commits offsets only after a batch has been handled.
package consumer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/batch"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/retry"
)

const defaultPendingBatches = 2

// Headers added to every record produced to the dead-letter topic.
const (
	HeaderTopic     = "dlq-original-topic"
	HeaderPartition = "dlq-original-partition"
	HeaderOffset    = "dlq-original-offset"
	HeaderError     = "dlq-error"
)

// Sink persists a batch of events. A failed batch is written again in full,
// so Write must tolerate duplicates.
type Sink interface {
	Write(ctx context.Context, events []event.Event) error
}

// Client is the part of *kgo.Client the Consumer uses.
type Client interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// Config tunes batching, dead-lettering and shutdown.
type Config struct {
	DeadLetterTopic string
	BatchSize       int
	BatchTimeout    time.Duration
	// PendingBatches is how many full batches may wait for the flusher while
	// the next one is polled. Zero means 2.
	PendingBatches  int
	ShutdownTimeout time.Duration
}

// Consumer moves records from Kafka to a Sink with at-least-once delivery.
// Records whose payload is not a valid event, and batches the Sink keeps
// rejecting, are produced to the dead-letter topic instead.
type Consumer struct {
	client Client
	sink   Sink
	retry  retry.Policy
	cfg    Config
	logger *slog.Logger
	batch  *batch.Batcher[pending]
}

type pending struct {
	record *kgo.Record
	event  event.Event
	err    error
}

// ClientOptions returns the kgo options the Consumer relies on: group
// membership with manual commits, and rebalances held back while a polled
// batch is still uncommitted.
func ClientOptions(group, topic string, logger *slog.Logger) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(logPartitions(logger, "partitions assigned")),
		kgo.OnPartitionsRevoked(logPartitions(logger, "partitions revoked")),
		kgo.OnPartitionsLost(logPartitions(logger, "partitions lost")),
	}
}

// New returns a Consumer. The client must be created with ClientOptions.
func New(client Client, sink Sink, policy retry.Policy, cfg Config, logger *slog.Logger) *Consumer {
	cfg.PendingBatches = cmp.Or(cfg.PendingBatches, defaultPendingBatches)
	return &Consumer{
		client: client,
		sink:   sink,
		retry:  policy,
		cfg:    cfg,
		logger: logger,
		batch:  batch.New[pending](cfg.BatchSize, cfg.BatchTimeout),
	}
}

// Run consumes until ctx is done, then flushes and commits the pending batch
// within Config.ShutdownTimeout. Full batches are written by a background
// goroutine while the next one is polled. An error means some records were
// left uncommitted and will be delivered again.
func (c *Consumer) Run(ctx context.Context) error {
	flushCtx, cancel := withGrace(ctx, c.cfg.ShutdownTimeout)
	defer cancel()

	batches := make(chan []pending, c.cfg.PendingBatches)
	flushed := make(chan error, 1)
	go func() {
		flushed <- c.flushLoop(flushCtx, batches)
	}()

	for ctx.Err() == nil {
		if err := c.poll(ctx); err != nil {
			close(batches)
			return errors.Join(err, <-flushed)
		}
		if c.batch.Due(time.Now()) {
			select {
			case batches <- c.batch.Drain():
			default:
				c.logger.WarnContext(ctx, "flusher is busy, keeping batch", "records", c.batch.Len())
			}
		}
		if c.batch.Len() == 0 {
			c.client.AllowRebalance()
		}
	}

	c.logger.InfoContext(flushCtx, "flushing pending batch", "records", c.batch.Len())
	batches <- c.batch.Drain()
	close(batches)
	if err := <-flushed; err != nil {
		return err
	}
	c.client.AllowRebalance()
	return nil
}

// flushLoop writes batches in order until the channel is closed. A failed
// batch is logged and the loop moves on to the next one; the errors are
// returned together once the channel is drained.
func (c *Consumer) flushLoop(ctx context.Context, batches <-chan []pending) error {
	var errs []error
	for items := range batches {
		if err := c.flush(ctx, items); err != nil {
			c.logger.ErrorContext(ctx, "flush failed", "records", len(items), "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Consumer) poll(ctx context.Context) error {
	pollCtx, cancel := c.pollContext(ctx)
	defer cancel()

	fetches := c.client.PollRecords(pollCtx, c.batch.Room())
	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
			continue
		}
		if errors.Is(fe.Err, kgo.ErrClientClosed) {
			return fmt.Errorf("poll: %w", fe.Err)
		}
		c.logger.ErrorContext(ctx, "fetch failed", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
	}

	now := time.Now()
	fetches.EachRecord(func(r *kgo.Record) {
		e, err := event.Decode(r.Value)
		if err != nil {
			c.logger.ErrorContext(ctx, "invalid event",
				"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
		}
		c.batch.Add(now, pending{record: r, event: e, err: err})
	})
	return nil
}

func (c *Consumer) pollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := c.batch.Deadline(); ok {
		return context.WithDeadline(ctx, deadline)
	}
	return context.WithCancel(ctx)
}

func (c *Consumer) flush(ctx context.Context, items []pending) error {
	if len(items) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(items))
	var (
		valid  []*kgo.Record
		events []event.Event
		dead   []*kgo.Record
	)
	for _, it := range items {
		records = append(records, it.record)
		if it.err != nil {
			dead = append(dead, c.deadLetter(it.record, it.err))
			continue
		}
		valid = append(valid, it.record)
		events = append(events, it.event)
	}

	if err := c.write(ctx, events); err != nil {
		if !errors.Is(err, retry.ErrExhausted) || ctx.Err() != nil {
			return fmt.Errorf("write %d events: %w", len(events), err)
		}
		c.logger.ErrorContext(ctx, "dead-lettering batch", "events", len(events), "error", err)
		for _, r := range valid {
			dead = append(dead, c.deadLetter(r, err))
		}
	}

	if len(dead) > 0 {
		if err := c.client.ProduceSync(ctx, dead...).FirstErr(); err != nil {
			return fmt.Errorf("produce %d dead letters: %w", len(dead), err)
		}
	}

	if err := c.client.CommitRecords(ctx, records...); err != nil {
		c.logger.ErrorContext(ctx, "commit failed", "records", len(records), "error", err)
		return nil
	}
	c.logger.InfoContext(ctx, "batch committed",
		"records", len(records), "written", len(records)-len(dead), "dead_lettered", len(dead))
	return nil
}

func (c *Consumer) write(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	return c.retry.Do(ctx, func(ctx context.Context, attempt int) error {
		err := c.sink.Write(ctx, events)
		if err != nil {
			c.logger.ErrorContext(ctx, "sink write failed", "attempt", attempt, "events", len(events), "error", err)
		}
		return err
	})
}

func (c *Consumer) deadLetter(r *kgo.Record, cause error) *kgo.Record {
	return &kgo.Record{
		Topic: c.cfg.DeadLetterTopic,
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

func withGrace(parent context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, func() {
		select {
		case <-time.After(grace):
		case <-ctx.Done():
		}
		cancel()
	})
	return ctx, func() {
		stop()
		cancel()
	}
}

func logPartitions(logger *slog.Logger, msg string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, _ *kgo.Client, partitions map[string][]int32) {
		logger.InfoContext(ctx, msg, "partitions", partitions)
	}
}
