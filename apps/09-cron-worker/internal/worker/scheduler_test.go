package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRunnerServe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		jobTakes      time.Duration
		grace         time.Duration
		wantErr       error
		wantCancelled bool
	}{
		{
			name:     "running job finishes within grace",
			jobTakes: 50 * time.Millisecond,
			grace:    10 * time.Second,
		},
		{
			name:          "running job cancelled after grace",
			jobTakes:      time.Hour,
			grace:         50 * time.Millisecond,
			wantErr:       ErrShutdownTimeout,
			wantCancelled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner, err := NewRunner(slog.New(slog.DiscardHandler), &fakeLocker{acquired: true},
				&fakeClock{}, prometheus.NewRegistry())
			require.NoError(t, err)

			started := make(chan struct{}, 1)
			jobErr := make(chan error, 1)
			job := jobFunc(func(ctx context.Context) error {
				select {
				case started <- struct{}{}:
				default:
					return nil
				}
				select {
				case <-time.After(tt.jobTakes):
					jobErr <- nil
				case <-ctx.Done():
					jobErr <- ctx.Err()
				}
				return nil
			})

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			served := make(chan error, 1)
			go func() {
				served <- runner.Serve(ctx, []Entry{{Name: "job", Spec: "* * * * * *", Job: job}}, tt.grace)
			}()

			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("job was not started by the scheduler")
			}
			cancel()

			require.ErrorIs(t, <-served, tt.wantErr)
			if tt.wantCancelled {
				require.ErrorIs(t, <-jobErr, context.Canceled)
			} else {
				require.NoError(t, <-jobErr)
			}
		})
	}
}

func TestRunnerServeRejectsInvalidSpec(t *testing.T) {
	t.Parallel()

	runner, err := NewRunner(slog.New(slog.DiscardHandler), &fakeLocker{}, &fakeClock{}, prometheus.NewRegistry())
	require.NoError(t, err)

	err = runner.Serve(t.Context(), []Entry{{Name: "job", Spec: "*/5 * * * *"}}, time.Second)
	require.ErrorContains(t, err, "schedule job job")
}
