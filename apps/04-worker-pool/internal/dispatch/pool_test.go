package dispatch_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/backoff"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
)

const hang = 0

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func testConfig() dispatch.Config {
	return dispatch.Config{
		Workers:        2,
		MaxAttempts:    3,
		MaxPerHost:     4,
		AttemptTimeout: time.Second,
		Backoff: backoff.Policy{
			Base: time.Millisecond,
			Max:  4 * time.Millisecond,
			Rand: func(n int64) int64 { return n - 1 },
		},
	}
}

func newPool(srv *httptest.Server, cfg dispatch.Config) *dispatch.Pool {
	return dispatch.NewPool(srv.Client(), cfg, slog.New(slog.DiscardHandler))
}

func newQueue(t *testing.T, srv *httptest.Server, n int) *dispatch.Queue {
	t.Helper()

	q := dispatch.NewQueue(n)
	for i := range n {
		require.NoError(t, q.Push(dispatch.Task{
			ID:      "task-" + strconv.Itoa(i),
			URL:     srv.URL,
			Payload: []byte(`{"event":"ping"}`),
		}))
	}
	return q
}

func collect(ctx context.Context, p *dispatch.Pool, tasks <-chan dispatch.Task) (map[string]dispatch.Result, error) {
	errc := make(chan error, 1)
	go func() {
		errc <- p.Run(ctx, tasks)
	}()

	results := make(map[string]dispatch.Result)
	for res := range p.Results() {
		results[res.TaskID] = res
	}
	return results, <-errc
}

func TestPoolDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		responses    []int
		attemptLimit time.Duration
		wantStatus   dispatch.Status
		wantAttempts int
		wantErr      error
		wantCode     int
	}{
		{
			name:         "delivers on first attempt",
			responses:    []int{http.StatusOK},
			wantStatus:   dispatch.StatusDelivered,
			wantAttempts: 1,
		},
		{
			name:         "retries server errors then succeeds",
			responses:    []int{http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusNoContent},
			wantStatus:   dispatch.StatusDelivered,
			wantAttempts: 3,
		},
		{
			name:         "retries rate limiting",
			responses:    []int{http.StatusTooManyRequests, http.StatusAccepted},
			wantStatus:   dispatch.StatusDelivered,
			wantAttempts: 2,
		},
		{
			name:         "retries attempt timeout",
			responses:    []int{hang, http.StatusOK},
			attemptLimit: 50 * time.Millisecond,
			wantStatus:   dispatch.StatusDelivered,
			wantAttempts: 2,
		},
		{
			name:         "client error is permanent",
			responses:    []int{http.StatusBadRequest},
			wantStatus:   dispatch.StatusFailed,
			wantAttempts: 1,
			wantErr:      dispatch.ErrPermanent,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "gives up after max attempts",
			responses:    []int{http.StatusBadGateway},
			wantStatus:   dispatch.StatusFailed,
			wantAttempts: 3,
			wantErr:      dispatch.ErrRetriesExhausted,
			wantCode:     http.StatusBadGateway,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				calls    atomic.Int32
				mu       sync.Mutex
				keys     []string
				payloads []string
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				keys = append(keys, r.Header.Get("Idempotency-Key"))
				payloads = append(payloads, string(body))
				mu.Unlock()

				n := int(calls.Add(1)) - 1
				code := tt.responses[min(n, len(tt.responses)-1)]
				if code == hang {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)

			cfg := testConfig()
			if tt.attemptLimit > 0 {
				cfg.AttemptTimeout = tt.attemptLimit
			}
			q := newQueue(t, srv, 1)
			q.Close()

			results, err := collect(t.Context(), newPool(srv, cfg), q.Tasks())
			require.NoError(t, err)
			require.Len(t, results, 1)

			res := results["task-0"]
			require.Equal(t, tt.wantStatus, res.Status)
			require.Equal(t, tt.wantAttempts, res.Attempts)
			require.EqualValues(t, tt.wantAttempts, calls.Load())
			if tt.wantErr == nil {
				require.NoError(t, res.Err)
			} else {
				require.ErrorIs(t, res.Err, tt.wantErr)
				var statusErr *dispatch.StatusError
				require.ErrorAs(t, res.Err, &statusErr)
				require.Equal(t, tt.wantCode, statusErr.Code)
			}

			mu.Lock()
			defer mu.Unlock()
			for i := range keys {
				require.Equal(t, "task-0", keys[i])
				require.JSONEq(t, `{"event":"ping"}`, payloads[i])
			}
		})
	}
}

func TestPoolCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    int
		backoff time.Duration
	}{
		{name: "aborts in-flight request", code: hang},
		{name: "aborts backoff sleep", code: http.StatusServiceUnavailable, backoff: time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			started := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case started <- struct{}{}:
				default:
				}
				if tt.code == hang {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(tt.code)
			}))
			t.Cleanup(srv.Close)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			cfg := testConfig()
			cfg.Workers = 1
			if tt.backoff > 0 {
				cfg.Backoff = backoff.Policy{Base: tt.backoff, Max: tt.backoff, Rand: func(n int64) int64 {
					cancel()
					return n - 1
				}}
			} else {
				go func() {
					<-started
					cancel()
				}()
			}
			q := newQueue(t, srv, 3)
			q.Close()

			results, err := collect(ctx, newPool(srv, cfg), q.Tasks())
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, results, 3)
			for id, res := range results {
				require.Equal(t, dispatch.StatusCanceled, res.Status, id)
				require.ErrorIs(t, res.Err, context.Canceled, id)
			}
			require.Equal(t, 1, results["task-0"].Attempts)
			require.Zero(t, results["task-1"].Attempts)
			require.Zero(t, results["task-2"].Attempts)
		})
	}
}

func TestPoolDrainsQueueOnClose(t *testing.T) {
	t.Parallel()

	var inFlight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	q := newQueue(t, srv, 6)
	p := newPool(srv, cfg)

	errc := make(chan error, 1)
	go func() {
		errc <- p.Run(t.Context(), q.Tasks())
	}()
	q.Close()

	var delivered int
	for res := range p.Results() {
		require.Equal(t, dispatch.StatusDelivered, res.Status, res.TaskID)
		delivered++
	}
	require.NoError(t, <-errc)
	require.Equal(t, 6, delivered)
	require.LessOrEqual(t, int(peak.Load()), cfg.Workers)
}

func TestPoolLimitsConcurrencyPerHost(t *testing.T) {
	t.Parallel()

	var inFlight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	cfg.MaxPerHost = 2
	q := newQueue(t, srv, 6)
	q.Close()

	results, err := collect(t.Context(), newPool(srv, cfg), q.Tasks())
	require.NoError(t, err)
	require.Len(t, results, 6)
	for id, res := range results {
		require.Equal(t, dispatch.StatusDelivered, res.Status, id)
	}
	require.LessOrEqual(t, int(peak.Load()), cfg.MaxPerHost)
}
