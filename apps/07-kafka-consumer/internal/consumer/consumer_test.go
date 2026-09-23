package consumer_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/consumer"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/retry"
)

const (
	topic    = "events"
	dlqTopic = "events.dlq"
)

var (
	errSinkDown = errors.New("sink down")
	errBroker   = errors.New("broker unavailable")
)

type fakeClient struct {
	queue           []*kgo.Record
	stop            context.CancelFunc
	produceErr      error
	produceFailures int
	produceCalls    int
	produced        []*kgo.Record
	commits         [][]int64
}

func (f *fakeClient) PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches {
	if len(f.queue) == 0 {
		f.stop()
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	n := min(maxPollRecords, len(f.queue))
	records := f.queue[:n]
	f.queue = f.queue[n:]
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: 0, Records: records}},
	}}}}
}

func (f *fakeClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.produceCalls++
	err := f.produceErr
	if f.produceCalls <= f.produceFailures {
		err = errBroker
	}
	results := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		results = append(results, kgo.ProduceResult{Record: r, Err: err})
	}
	if err == nil {
		f.produced = append(f.produced, rs...)
	}
	return results
}

func (f *fakeClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	offsets := make([]int64, 0, len(rs))
	for _, r := range rs {
		offsets = append(offsets, r.Offset)
	}
	f.commits = append(f.commits, offsets)
	return nil
}

func (f *fakeClient) AllowRebalance() {}

type fakeSink struct {
	failures int
	delay    time.Duration
	calls    int
	written  []string
}

func (s *fakeSink) Write(_ context.Context, events []event.Event) error {
	time.Sleep(s.delay)
	s.calls++
	if s.calls <= s.failures {
		return errSinkDown
	}
	for _, e := range events {
		s.written = append(s.written, e.ID)
	}
	return nil
}

func validEvent(id string) string {
	return fmt.Sprintf(`{"id":%q,"type":"order.created","occurred_at":"2026-09-01T10:00:00Z"}`, id)
}

func TestConsumerRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		values          []string
		batchSize       int
		retryDelay      time.Duration
		shutdownTimeout time.Duration
		sinkFailures    int
		sinkDelay       time.Duration
		produceErr      error
		produceFailures int
		wantErr         bool
		wantSinkCalls   int
		wantWritten     []string
		wantDead        []int64
		wantDeadError   string
		wantCommits     [][]int64
	}{
		{
			name:          "pending batch is written and committed on shutdown",
			values:        []string{validEvent("a"), validEvent("b"), validEvent("c")},
			wantSinkCalls: 1,
			wantWritten:   []string{"a", "b", "c"},
			wantCommits:   [][]int64{{0, 1, 2}},
		},
		{
			name:          "full batches are flushed before shutdown",
			values:        []string{validEvent("a"), validEvent("b"), validEvent("c"), validEvent("d"), validEvent("e")},
			batchSize:     2,
			wantSinkCalls: 3,
			wantWritten:   []string{"a", "b", "c", "d", "e"},
			wantCommits:   [][]int64{{0, 1}, {2, 3}, {4}},
		},
		{
			name:          "next batch is polled while the previous one is written",
			values:        []string{validEvent("a"), validEvent("b"), validEvent("c"), validEvent("d")},
			batchSize:     2,
			sinkDelay:     20 * time.Millisecond,
			wantSinkCalls: 2,
			wantWritten:   []string{"a", "b", "c", "d"},
			wantCommits:   [][]int64{{0, 1}, {2, 3}},
		},
		{
			name:          "invalid json skips the sink",
			values:        []string{validEvent("a"), `{"id":`, validEvent("c")},
			wantSinkCalls: 1,
			wantWritten:   []string{"a", "c"},
			wantDead:      []int64{1},
			wantDeadError: "invalid event",
			wantCommits:   [][]int64{{0, 1, 2}},
		},
		{
			name:          "sink recovers within max attempts",
			values:        []string{validEvent("a"), validEvent("b")},
			sinkFailures:  2,
			wantSinkCalls: 3,
			wantWritten:   []string{"a", "b"},
			wantCommits:   [][]int64{{0, 1}},
		},
		{
			name:          "batch is dead-lettered after max attempts",
			values:        []string{validEvent("a"), validEvent("b")},
			sinkFailures:  3,
			wantSinkCalls: 3,
			wantDead:      []int64{0, 1},
			wantDeadError: "sink down",
			wantCommits:   [][]int64{{0, 1}},
		},
		{
			name:          "failed dead-letter produce leaves batch uncommitted",
			values:        []string{validEvent("a"), validEvent("b")},
			sinkFailures:  3,
			produceErr:    errBroker,
			wantErr:       true,
			wantSinkCalls: 3,
		},
		{
			name:            "a failed flush does not stop the consumer",
			values:          []string{validEvent("a"), validEvent("b"), validEvent("c"), validEvent("d")},
			batchSize:       2,
			sinkFailures:    3,
			produceFailures: 1,
			wantErr:         true,
			wantSinkCalls:   4,
			wantWritten:     []string{"c", "d"},
			wantCommits:     [][]int64{{2, 3}},
		},
		{
			name:            "shutdown timeout abandons a failing batch",
			values:          []string{validEvent("a")},
			retryDelay:      time.Hour,
			shutdownTimeout: time.Millisecond,
			sinkFailures:    3,
			wantErr:         true,
			wantSinkCalls:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			client := &fakeClient{stop: cancel, produceErr: tt.produceErr, produceFailures: tt.produceFailures}
			for i, v := range tt.values {
				client.queue = append(client.queue, &kgo.Record{
					Topic:   topic,
					Offset:  int64(i),
					Key:     []byte("key-" + strconv.Itoa(i)),
					Value:   []byte(v),
					Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("t-1")}},
				})
			}
			sink := &fakeSink{failures: tt.sinkFailures, delay: tt.sinkDelay}
			cfg := consumer.Config{
				DeadLetterTopic: dlqTopic,
				BatchSize:       cmp.Or(tt.batchSize, 100),
				BatchTimeout:    time.Hour,
				ShutdownTimeout: cmp.Or(tt.shutdownTimeout, time.Minute),
			}
			policy := retry.New(3, tt.retryDelay, tt.retryDelay)

			err := consumer.New(client, sink, policy, cfg, slog.New(slog.DiscardHandler)).Run(ctx)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantSinkCalls, sink.calls)
			require.Equal(t, tt.wantWritten, sink.written)
			require.Equal(t, tt.wantCommits, client.commits)

			require.Len(t, client.produced, len(tt.wantDead))
			for i, r := range client.produced {
				require.Equal(t, dlqTopic, r.Topic)
				require.Equal(t, "key-"+strconv.FormatInt(tt.wantDead[i], 10), string(r.Key))
				require.Equal(t, "t-1", header(t, r, "trace-id"))
				require.Equal(t, topic, header(t, r, consumer.HeaderTopic))
				require.Equal(t, "0", header(t, r, consumer.HeaderPartition))
				require.Equal(t, strconv.FormatInt(tt.wantDead[i], 10), header(t, r, consumer.HeaderOffset))
				require.Contains(t, header(t, r, consumer.HeaderError), tt.wantDeadError)
			}
		})
	}
}

func header(t *testing.T, r *kgo.Record, key string) string {
	t.Helper()
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	t.Fatalf("record has no %q header", key)
	return ""
}
