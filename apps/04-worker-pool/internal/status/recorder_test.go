package status

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
)

func TestRecorderRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result dispatch.Result
		want   Record
	}{
		{
			name:   "delivered",
			result: dispatch.Result{TaskID: "a", Status: dispatch.StatusDelivered, Attempts: 2},
			want:   Record{ID: "a", URL: "http://target/a", Status: dispatch.StatusDelivered, Attempts: 2},
		},
		{
			name:   "failed keeps the error",
			result: dispatch.Result{TaskID: "a", Status: dispatch.StatusFailed, Attempts: 5, Err: errors.New("boom")},
			want:   Record{ID: "a", URL: "http://target/a", Status: dispatch.StatusFailed, Attempts: 5, Error: "boom"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			created := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			finished := created.Add(time.Second)
			r := NewRecorder(time.Hour)
			r.now = func() time.Time { return created }
			r.Track(dispatch.Task{ID: "a", URL: "http://target/a"})
			r.now = func() time.Time { return finished }

			results := make(chan dispatch.Result, 2)
			results <- tt.result
			results <- dispatch.Result{TaskID: "forgotten", Status: dispatch.StatusDelivered}
			close(results)
			r.Run(results)

			tt.want.CreatedAt = created
			tt.want.UpdatedAt = finished
			got, ok := r.Get("a")
			require.True(t, ok)
			require.Equal(t, tt.want, got)
			_, ok = r.Get("forgotten")
			require.False(t, ok)
		})
	}
}

func TestRecorderEvict(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		status   dispatch.Status
		updated  time.Time
		wantKept bool
	}{
		{name: "recent finished record is kept", status: dispatch.StatusDelivered, updated: now.Add(-time.Minute), wantKept: true},
		{name: "expired finished record is evicted", status: dispatch.StatusFailed, updated: now.Add(-2 * time.Hour)},
		{name: "expired queued record is kept", status: dispatch.StatusQueued, updated: now.Add(-2 * time.Hour), wantKept: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := NewRecorder(time.Hour)
			r.now = func() time.Time { return now }
			r.records["a"] = Record{ID: "a", Status: tt.status, UpdatedAt: tt.updated}

			r.evict()

			_, ok := r.Get("a")
			require.Equal(t, tt.wantKept, ok)
		})
	}
}
