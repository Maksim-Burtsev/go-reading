package batcher

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
)

var errInsert = errors.New("clickhouse unavailable")

type fakeInserter struct {
	mu       sync.Mutex
	failures int
	batches  [][]event.Event
}

func (f *fakeInserter) InsertEvents(_ context.Context, events []event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failures > 0 {
		f.failures--
		return errInsert
	}
	f.batches = append(f.batches, slices.Clone(events))
	return nil
}

func (f *fakeInserter) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	sizes := make([]int, 0, len(f.batches))
	for _, b := range f.batches {
		sizes = append(sizes, len(b))
	}
	return sizes
}

func testConfig() Config {
	return Config{
		BufferSize:    16,
		BatchSize:     4,
		FlushInterval: time.Hour,
		MaxAttempts:   3,
		RetryBackoff:  time.Millisecond,
		DrainTimeout:  time.Second,
	}
}

func newTestBatcher(t *testing.T, cfg Config, ins Inserter) *Batcher {
	t.Helper()

	b, err := New(cfg, ins, prometheus.NewRegistry(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	return b
}

func start(t *testing.T, b *Batcher) (stop func() error) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	return func() error {
		cancel()
		return <-done
	}
}

func TestEnqueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prefill int
		closed  bool
		events  int
		wantErr error
		wantLen int
	}{
		{name: "fits", events: 3, wantLen: 3},
		{name: "fills buffer exactly", prefill: 10, events: 6, wantLen: 16},
		{name: "buffer full keeps nothing", prefill: 14, events: 3, wantErr: ErrBufferFull, wantLen: 14},
		{name: "larger than buffer", events: 17, wantErr: ErrTooLarge},
		{name: "closed", closed: true, events: 1, wantErr: ErrClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBatcher(t, testConfig(), &fakeInserter{})
			require.NoError(t, b.Enqueue(make([]event.Event, tt.prefill)))
			if tt.closed {
				require.NoError(t, start(t, b)())
			}

			err := b.Enqueue(make([]event.Event, tt.events))
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantLen, b.Len())
		})
	}
}

func TestRunFlushes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		batchSize     int
		flushInterval time.Duration
		events        int
		waitFor       []int
		want          []int
	}{
		{
			name:          "by size, remainder on shutdown",
			batchSize:     3,
			flushInterval: time.Hour,
			events:        7,
			waitFor:       []int{3, 3},
			want:          []int{3, 3, 1},
		},
		{
			name:          "by interval",
			batchSize:     100,
			flushInterval: 10 * time.Millisecond,
			events:        2,
			waitFor:       []int{2},
			want:          []int{2},
		},
		{
			name:          "drain on shutdown",
			batchSize:     4,
			flushInterval: time.Hour,
			events:        10,
			want:          []int{4, 4, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.BatchSize = tt.batchSize
			cfg.FlushInterval = tt.flushInterval
			ins := &fakeInserter{}
			b := newTestBatcher(t, cfg, ins)

			require.NoError(t, b.Enqueue(make([]event.Event, tt.events)))
			stop := start(t, b)
			if tt.waitFor != nil {
				require.Eventually(t, func() bool {
					return slices.Equal(ins.sizes(), tt.waitFor)
				}, time.Second, time.Millisecond)
			}

			require.NoError(t, stop())
			require.Equal(t, tt.want, ins.sizes())
			require.Zero(t, b.Len())
		})
	}
}

func TestRunRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		failures        int
		wantBatches     []int
		wantFlushErrors float64
		wantDropped     float64
		wantErr         bool
	}{
		{name: "succeeds first time", wantBatches: []int{2}},
		{name: "recovers after retries", failures: 2, wantBatches: []int{2}, wantFlushErrors: 2},
		{
			name:            "drops after max attempts",
			failures:        5,
			wantBatches:     []int{},
			wantFlushErrors: 3,
			wantDropped:     2,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ins := &fakeInserter{failures: tt.failures}
			b := newTestBatcher(t, testConfig(), ins)

			require.NoError(t, b.Enqueue(make([]event.Event, 2)))
			err := start(t, b)()

			if tt.wantErr {
				require.ErrorIs(t, err, errInsert)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantBatches, ins.sizes())
			require.InDelta(t, tt.wantFlushErrors, testutil.ToFloat64(b.flushErrors), 0)
			require.InDelta(t, tt.wantDropped, testutil.ToFloat64(b.dropped), 0)
		})
	}
}
