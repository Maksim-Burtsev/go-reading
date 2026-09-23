package pipeline_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/ledger"
	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/pipeline"
)

const (
	topic    = "events"
	dlqTopic = "events.dlq"
)

var (
	errClickHouseDown = errors.New("clickhouse down")
	errRejected       = errors.New("clickhouse rejected the block")
	errPostgresDown   = errors.New("postgres down")
	errBroker         = errors.New("broker unavailable")
	errCoordinator    = errors.New("coordinator not available")
)

type fakeClient struct {
	queue          []*kgo.Record
	stop           context.CancelFunc
	produceErr     error
	produced       []*kgo.Record
	commitFailures int
	commitCalls    int
	commits        [][]string
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
		Partitions: []kgo.FetchPartition{{Records: records}},
	}}}}
}

func (f *fakeClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		results = append(results, kgo.ProduceResult{Record: r, Err: f.produceErr})
	}
	if f.produceErr == nil {
		f.produced = append(f.produced, rs...)
	}
	return results
}

func (f *fakeClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.commitCalls++
	if f.commitCalls <= f.commitFailures {
		return errCoordinator
	}
	var positions []string
	for _, r := range rs {
		positions = append(positions, position(r))
	}
	f.commits = append(f.commits, positions)
	return nil
}

func (f *fakeClient) AllowRebalance() {}

type fakeInserter struct {
	failures int
	hang     bool
	reject   []string
	calls    int
	inserted []string
}

func (f *fakeInserter) InsertEvents(ctx context.Context, events []event.Event) error {
	f.calls++
	if f.hang {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return errors.New("insert got no deadline")
		}
	}
	if f.calls <= f.failures {
		return errClickHouseDown
	}
	for _, e := range events {
		if slices.Contains(f.reject, e.ID) {
			return errRejected
		}
	}
	for _, e := range events {
		f.inserted = append(f.inserted, e.ID)
	}
	return nil
}

type fakeLedger struct {
	failures int
	calls    int
	batches  []ledger.Batch
}

func (f *fakeLedger) Record(_ context.Context, b ledger.Batch) error {
	f.calls++
	if f.calls <= f.failures {
		return errPostgresDown
	}
	b.InsertDuration = 0
	f.batches = append(f.batches, b)
	return nil
}

func validEvent(id string) string {
	return fmt.Sprintf(`{"id":%q,"type":"order.created","occurred_at":"2026-09-01T10:00:00Z"}`, id)
}

func records(partition int32, values ...string) []*kgo.Record {
	rs := make([]*kgo.Record, 0, len(values))
	for i, v := range values {
		rs = append(rs, &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Offset:    int64(10 + i),
			Key:       []byte(fmt.Sprintf("key-%d-%d", partition, i)),
			Value:     []byte(v),
			Headers:   []kgo.RecordHeader{{Key: "trace-id", Value: []byte("t-1")}},
		})
	}
	return rs
}

func position(r *kgo.Record) string {
	return fmt.Sprintf("%d/%d", r.Partition, r.Offset)
}

func TestPipelineRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		records         []*kgo.Record
		batchSize       int
		retryBackoff    time.Duration
		attemptTimeout  time.Duration
		shutdownTimeout time.Duration
		insertFailures  int
		insertHangs     bool
		reject          []string
		ledgerFailures  int
		commitFailures  int
		produceErr      error
		wantErr         error
		wantInserted    []string
		wantDead        []string
		wantDeadError   string
		wantBatches     []ledger.Batch
		wantCommits     [][]string
		wantMetrics     map[string]float64
	}{
		{
			name:         "pending batch is flushed and committed on shutdown",
			records:      records(0, validEvent("a"), validEvent("b"), validEvent("c")),
			wantInserted: []string{"a", "b", "c"},
			wantBatches: []ledger.Batch{{
				Status:     ledger.StatusInserted,
				Records:    3,
				Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 12, Records: 3}},
			}},
			wantCommits: [][]string{{"0/10", "0/11", "0/12"}},
			wantMetrics: map[string]float64{"records_consumed_total": 3, "batches_flushed_total/inserted": 1},
		},
		{
			name:         "full batches are flushed before shutdown",
			records:      records(0, validEvent("a"), validEvent("b"), validEvent("c")),
			batchSize:    2,
			wantInserted: []string{"a", "b", "c"},
			wantBatches: []ledger.Batch{
				{
					Status:     ledger.StatusInserted,
					Records:    2,
					Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 11, Records: 2}},
				},
				{
					Status:     ledger.StatusInserted,
					Records:    1,
					Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 12, LastOffset: 12, Records: 1}},
				},
			},
			wantCommits: [][]string{{"0/10", "0/11"}, {"0/12"}},
			wantMetrics: map[string]float64{"batches_flushed_total/inserted": 2},
		},
		{
			name:         "batch spanning partitions records a range per partition",
			records:      append(records(0, validEvent("a"), validEvent("b")), records(3, validEvent("c"))...),
			wantInserted: []string{"a", "b", "c"},
			wantBatches: []ledger.Batch{{
				Status:  ledger.StatusInserted,
				Records: 3,
				Partitions: []ledger.Partition{
					{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 11, Records: 2},
					{Topic: topic, Partition: 3, FirstOffset: 10, LastOffset: 10, Records: 1},
				},
			}},
			wantCommits: [][]string{{"0/10", "0/11", "3/10"}},
		},
		{
			name:          "invalid event is dead-lettered with its batch",
			records:       records(0, validEvent("a"), `{"id":`, validEvent("c")),
			wantInserted:  []string{"a", "c"},
			wantDead:      []string{"0/11"},
			wantDeadError: "invalid event",
			wantBatches: []ledger.Batch{{
				Status:       ledger.StatusInserted,
				Records:      3,
				DeadLettered: 1,
				Partitions:   []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 12, Records: 3}},
			}},
			wantCommits: [][]string{{"0/10", "0/11", "0/12"}},
			wantMetrics: map[string]float64{"dead_letters_total/invalid": 1, "dead_letters_total/insert_failed": 0},
		},
		{
			name:           "insert recovers within max attempts",
			records:        records(0, validEvent("a")),
			insertFailures: 2,
			wantInserted:   []string{"a"},
			wantBatches: []ledger.Batch{{
				Status:     ledger.StatusInserted,
				Records:    1,
				Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 10, Records: 1}},
			}},
			wantCommits: [][]string{{"0/10"}},
		},
		{
			name:           "insert that recovers after max attempts is not split",
			records:        records(0, validEvent("a"), validEvent("b")),
			insertFailures: 3,
			wantInserted:   []string{"a", "b"},
			wantBatches: []ledger.Batch{{
				Status:     ledger.StatusInserted,
				Records:    2,
				Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 11, Records: 2}},
			}},
			wantCommits: [][]string{{"0/10", "0/11"}},
		},
		{
			name:          "only the rejected event is dead-lettered",
			records:       records(0, validEvent("a"), validEvent("b"), validEvent("c")),
			reject:        []string{"a"},
			wantInserted:  []string{"b", "c"},
			wantDead:      []string{"0/10"},
			wantDeadError: "clickhouse rejected the block",
			wantBatches: []ledger.Batch{{
				Status:       ledger.StatusPartiallyDeadLettered,
				Records:      3,
				DeadLettered: 1,
				Partitions:   []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 12, Records: 3}},
			}},
			wantCommits: [][]string{{"0/10", "0/11", "0/12"}},
			wantMetrics: map[string]float64{"dead_letters_total/insert_failed": 1, "batches_flushed_total/partially_dead_lettered": 1},
		},
		{
			name:          "batch ClickHouse keeps rejecting is dead-lettered event by event",
			records:       records(0, validEvent("a"), validEvent("b")),
			reject:        []string{"a", "b"},
			wantDead:      []string{"0/10", "0/11"},
			wantDeadError: "clickhouse rejected the block",
			wantBatches: []ledger.Batch{{
				Status:       ledger.StatusDeadLettered,
				Records:      2,
				DeadLettered: 2,
				Partitions:   []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 11, Records: 2}},
			}},
			wantCommits: [][]string{{"0/10", "0/11"}},
			wantMetrics: map[string]float64{"dead_letters_total/insert_failed": 2, "batches_flushed_total/dead_lettered": 1},
		},
		{
			name:           "insert that hangs times out on every attempt and is dead-lettered",
			records:        records(0, validEvent("a")),
			attemptTimeout: 10 * time.Millisecond,
			insertHangs:    true,
			wantDead:       []string{"0/10"},
			wantDeadError:  "context deadline exceeded",
			wantBatches: []ledger.Batch{{
				Status:       ledger.StatusDeadLettered,
				Records:      1,
				DeadLettered: 1,
				Partitions:   []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 10, Records: 1}},
			}},
			wantCommits: [][]string{{"0/10"}},
			wantMetrics: map[string]float64{"dead_letters_total/insert_failed": 1},
		},
		{
			name:           "failed dead-letter produce leaves batch uncommitted",
			records:        records(0, validEvent("a")),
			insertFailures: 3,
			produceErr:     errBroker,
			wantErr:        errBroker,
		},
		{
			name:           "ledger recovers within max attempts",
			records:        records(0, validEvent("a")),
			ledgerFailures: 2,
			wantInserted:   []string{"a"},
			wantBatches: []ledger.Batch{{
				Status:     ledger.StatusInserted,
				Records:    1,
				Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 10, Records: 1}},
			}},
			wantCommits: [][]string{{"0/10"}},
		},
		{
			name:           "failed ledger write leaves inserted batch uncommitted",
			records:        records(0, validEvent("a")),
			ledgerFailures: 3,
			wantErr:        errPostgresDown,
			wantInserted:   []string{"a"},
		},
		{
			name:           "failed commit before shutdown is logged and consumption goes on",
			records:        records(0, validEvent("a"), validEvent("b"), validEvent("c")),
			batchSize:      2,
			commitFailures: 1,
			wantInserted:   []string{"a", "b", "c"},
			wantBatches: []ledger.Batch{
				{
					Status:     ledger.StatusInserted,
					Records:    2,
					Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 11, Records: 2}},
				},
				{
					Status:     ledger.StatusInserted,
					Records:    1,
					Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 12, LastOffset: 12, Records: 1}},
				},
			},
			wantCommits: [][]string{{"0/12"}},
		},
		{
			name:           "failed commit at shutdown fails the run",
			records:        records(0, validEvent("a")),
			commitFailures: 1,
			wantErr:        errCoordinator,
			wantInserted:   []string{"a"},
			wantBatches: []ledger.Batch{{
				Status:     ledger.StatusInserted,
				Records:    1,
				Partitions: []ledger.Partition{{Topic: topic, Partition: 0, FirstOffset: 10, LastOffset: 10, Records: 1}},
			}},
		},
		{
			name:            "shutdown timeout abandons a failing batch",
			records:         records(0, validEvent("a")),
			retryBackoff:    time.Hour,
			shutdownTimeout: time.Millisecond,
			insertFailures:  3,
			wantErr:         context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			client := &fakeClient{queue: tt.records, stop: cancel, produceErr: tt.produceErr, commitFailures: tt.commitFailures}
			inserter := &fakeInserter{failures: tt.insertFailures, hang: tt.insertHangs, reject: tt.reject}
			batches := &fakeLedger{failures: tt.ledgerFailures}
			reg := prometheus.NewRegistry()
			p, err := pipeline.New(client, inserter, batches, pipeline.Config{
				DeadLetterTopic: dlqTopic,
				BatchSize:       cmp.Or(tt.batchSize, 100),
				BatchTimeout:    time.Hour,
				MaxAttempts:     3,
				RetryBackoff:    tt.retryBackoff,
				AttemptTimeout:  cmp.Or(tt.attemptTimeout, time.Minute),
				ShutdownTimeout: cmp.Or(tt.shutdownTimeout, time.Minute),
			}, reg, slog.New(slog.DiscardHandler))
			require.NoError(t, err)

			err = p.Run(ctx)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantInserted, inserter.inserted)
			require.Equal(t, tt.wantBatches, batches.batches)
			require.Equal(t, tt.wantCommits, client.commits)

			var dead []string
			for _, r := range client.produced {
				require.Equal(t, dlqTopic, r.Topic)
				require.Equal(t, "t-1", header(t, r, "trace-id"))
				require.Equal(t, topic, header(t, r, pipeline.HeaderTopic))
				require.Contains(t, header(t, r, pipeline.HeaderError), tt.wantDeadError)
				dead = append(dead, header(t, r, pipeline.HeaderPartition)+"/"+header(t, r, pipeline.HeaderOffset))
			}
			require.Equal(t, tt.wantDead, dead)

			for name, want := range tt.wantMetrics {
				require.Equal(t, want, metricValue(t, reg, name), name)
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

func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		for _, m := range f.GetMetric() {
			id := f.GetName()
			for _, l := range m.GetLabel() {
				id += "/" + l.GetValue()
			}
			if id == "eventsink_"+name {
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
