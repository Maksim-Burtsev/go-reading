package batch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/batch"
)

func TestBatcher(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		size     int
		arrivals []time.Duration
		checkAt  time.Duration
		wantFull bool
		wantDue  bool
		wantRoom int
	}{
		{
			name:     "empty batch is never due",
			size:     3,
			checkAt:  time.Hour,
			wantRoom: 3,
		},
		{
			name:     "partial batch before timeout",
			size:     3,
			arrivals: []time.Duration{0, 400 * time.Millisecond},
			checkAt:  999 * time.Millisecond,
			wantRoom: 1,
		},
		{
			name:     "full batch is due at once",
			size:     3,
			arrivals: []time.Duration{0, 0, 0},
			wantFull: true,
			wantDue:  true,
		},
		{
			name:     "timeout counts from the first item",
			size:     3,
			arrivals: []time.Duration{0, 900 * time.Millisecond},
			checkAt:  time.Second,
			wantDue:  true,
			wantRoom: 1,
		},
		{
			name:     "overdue partial batch",
			size:     100,
			arrivals: []time.Duration{0},
			checkAt:  time.Minute,
			wantDue:  true,
			wantRoom: 99,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := batch.New[int](tt.size, time.Second)
			var full bool
			for i, at := range tt.arrivals {
				full = b.Add(start.Add(at), i)
			}

			require.Equal(t, tt.wantFull, full)
			require.Equal(t, tt.wantDue, b.Due(start.Add(tt.checkAt)))
			require.Equal(t, tt.wantRoom, b.Room())
			require.Equal(t, len(tt.arrivals), b.Len())
		})
	}
}

func TestBatcherDrain(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	b := batch.New[string](2, time.Second)
	b.Add(start, "a")
	b.Add(start.Add(time.Millisecond), "b")

	require.Equal(t, []string{"a", "b"}, b.Drain())
	require.Zero(t, b.Len())
	require.False(t, b.Due(start.Add(time.Hour)))
	_, ok := b.Deadline()
	require.False(t, ok)

	b.Add(start.Add(time.Minute), "c")
	deadline, ok := b.Deadline()
	require.True(t, ok)
	require.Equal(t, start.Add(time.Minute+time.Second), deadline)
}
