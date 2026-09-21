//go:build integration

package consumer_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/consumer"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/retry"
)

const validEvents = 150

type recordingSink struct {
	err error
	mu  sync.Mutex
	ids []string
}

func (s *recordingSink) Write(_ context.Context, events []event.Event) error {
	if s.err != nil {
		return s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		s.ids = append(s.ids, e.ID)
	}
	return nil
}

func (s *recordingSink) IDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ids)
}

func TestConsumerWithRedpanda(t *testing.T) {
	t.Parallel()

	ctr, err := redpanda.Run(t.Context(), "redpandadata/redpanda:v25.1.1")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	broker, err := ctr.KafkaSeedBroker(t.Context())
	require.NoError(t, err)

	tests := []struct {
		name          string
		sinkErr       error
		wantWritten   int
		wantDead      int
		wantSinkError int
	}{
		{
			name:        "events reach the sink",
			wantWritten: validEvents,
			wantDead:    1,
		},
		{
			name:          "failing sink sends records to the dead-letter topic",
			sinkErr:       errSinkDown,
			wantDead:      validEvents + 1,
			wantSinkError: validEvents,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			topic := "events-" + strconv.Itoa(i)
			dlq := topic + ".dlq"
			group := topic + "-sink"
			logger := slog.New(slog.NewTextHandler(t.Output(), nil))

			adm := kadm.NewClient(newClient(t, broker))
			created, err := adm.CreateTopics(ctx, 3, 1, nil, topic, dlq)
			require.NoError(t, err)
			require.NoError(t, created.Error())

			produced := produceEvents(t, newClient(t, broker), topic)

			sink := &recordingSink{err: tt.sinkErr}
			c := consumer.New(
				newClient(t, broker, consumer.ClientOptions(group, topic, logger)...),
				sink,
				retry.New(3, 10*time.Millisecond, 50*time.Millisecond),
				consumer.Config{
					DeadLetterTopic: dlq,
					BatchSize:       100,
					BatchTimeout:    200 * time.Millisecond,
					ShutdownTimeout: 5 * time.Second,
				},
				logger,
			)
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Run(runCtx) }()

			dead := readRecords(t, newClient(t, broker, kgo.ConsumeTopics(dlq)), tt.wantDead)
			require.Eventually(t, func() bool { return len(sink.IDs()) == tt.wantWritten },
				30*time.Second, 50*time.Millisecond)

			cancel()
			require.NoError(t, <-done)

			wantIDs := make([]string, 0, tt.wantWritten)
			for n := range tt.wantWritten {
				wantIDs = append(wantIDs, fmt.Sprintf("e-%d", n))
			}
			require.ElementsMatch(t, wantIDs, sink.IDs())

			var sinkErrors int
			for _, r := range dead {
				key := header(t, r, consumer.HeaderPartition) + "/" + header(t, r, consumer.HeaderOffset)
				original, ok := produced[key]
				require.True(t, ok, "unknown original record %s", key)
				require.Equal(t, original.Value, r.Value)
				require.Equal(t, topic, header(t, r, consumer.HeaderTopic))
				if strings.Contains(header(t, r, consumer.HeaderError), errSinkDown.Error()) {
					sinkErrors++
				}
			}
			require.Equal(t, tt.wantSinkError, sinkErrors)

			requireCommitted(t, adm, group, topic)
		})
	}
}

func newClient(t *testing.T, broker string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers(broker))...)
	require.NoError(t, err)
	t.Cleanup(cl.CloseAllowingRebalance)
	return cl
}

func produceEvents(t *testing.T, cl *kgo.Client, topic string) map[string]*kgo.Record {
	t.Helper()
	records := make([]*kgo.Record, 0, validEvents+1)
	for n := range validEvents {
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(n)),
			Value: []byte(validEvent(fmt.Sprintf("e-%d", n))),
		})
	}
	records = append(records, &kgo.Record{Topic: topic, Key: []byte("poison"), Value: []byte("not json")})
	require.NoError(t, cl.ProduceSync(t.Context(), records...).FirstErr())

	byPosition := make(map[string]*kgo.Record, len(records))
	for _, r := range records {
		byPosition[fmt.Sprintf("%d/%d", r.Partition, r.Offset)] = r
	}
	return byPosition
}

func readRecords(t *testing.T, cl *kgo.Client, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < n {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, fetches.Err(), "read %d of %d records", len(records), n)
		records = append(records, fetches.Records()...)
	}
	return records
}

func requireCommitted(t *testing.T, adm *kadm.Client, group, topic string) {
	t.Helper()
	committed, err := adm.FetchOffsets(t.Context(), group)
	require.NoError(t, err)
	require.NoError(t, committed.Error())
	ends, err := adm.ListEndOffsets(t.Context(), topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())

	ends.Each(func(end kadm.ListedOffset) {
		if end.Offset == 0 {
			return
		}
		c, ok := committed.Lookup(topic, end.Partition)
		require.True(t, ok, "partition %d has no committed offset", end.Partition)
		require.Equal(t, end.Offset, c.At, "partition %d", end.Partition)
	})
}
