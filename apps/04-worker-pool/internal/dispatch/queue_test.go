package dispatch_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
)

func TestQueuePush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		prefill int
		closed  bool
		wantErr error
	}{
		{name: "accepts while below capacity", size: 2, prefill: 1},
		{name: "rejects when full", size: 2, prefill: 2, wantErr: dispatch.ErrQueueFull},
		{name: "rejects after close", size: 2, closed: true, wantErr: dispatch.ErrQueueClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := dispatch.NewQueue(tt.size)
			for i := range tt.prefill {
				require.NoError(t, q.Push(dispatch.Task{ID: strconv.Itoa(i)}))
			}
			if tt.closed {
				q.Close()
			}

			err := q.Push(dispatch.Task{ID: "new"})
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestQueueCloseKeepsQueuedTasks(t *testing.T) {
	t.Parallel()

	q := dispatch.NewQueue(3)
	require.NoError(t, q.Push(dispatch.Task{ID: "a"}))
	require.NoError(t, q.Push(dispatch.Task{ID: "b"}))
	q.Close()
	q.Close()

	var ids []string
	for task := range q.Tasks() {
		ids = append(ids, task.ID)
	}
	require.Equal(t, []string{"a", "b"}, ids)
}

func TestQueueConcurrentPushAndClose(t *testing.T) {
	t.Parallel()

	q := dispatch.NewQueue(8)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			err := q.Push(dispatch.Task{ID: strconv.Itoa(i)})
			if err != nil && !errors.Is(err, dispatch.ErrQueueFull) && !errors.Is(err, dispatch.ErrQueueClosed) {
				t.Errorf("unexpected push error: %v", err)
			}
		})
	}
	q.Close()
	wg.Wait()

	require.LessOrEqual(t, q.Len(), 8)
}
