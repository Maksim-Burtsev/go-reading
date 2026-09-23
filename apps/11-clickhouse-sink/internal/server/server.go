// Package server exposes the sink's HTTP API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/batcher"
	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
)

const (
	maxBodyBytes  = 4 << 20
	healthTimeout = 2 * time.Second
)

var errTrailingData = errors.New("body must contain a single JSON array")

// Enqueuer accepts events for asynchronous storage.
type Enqueuer interface {
	Enqueue(events []event.Event) error
}

// Pinger checks the availability of the storage backend.
type Pinger interface {
	Ping(ctx context.Context) error
}

type ingestMetrics struct {
	received prometheus.Counter
	rejected *prometheus.CounterVec
}

type ingestResponse struct {
	Accepted int `json:"accepted"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type healthResponse struct {
	Status string `json:"status"`
}

// NewHandler builds the HTTP handler and registers its metrics with reg. now
// is the clock that event timestamps are validated against, and retryAfter is
// how long a client is asked to wait when the buffer is full.
func NewHandler(
	logger *slog.Logger,
	queue Enqueuer,
	db Pinger,
	reg *prometheus.Registry,
	now func() time.Time,
	retryAfter time.Duration,
) (http.Handler, error) {
	m := ingestMetrics{
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "sink",
			Name:      "events_received_total",
			Help:      "Events accepted into the buffer.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sink",
			Name:      "events_rejected_total",
			Help:      "Events refused, by reason.",
		}, []string{"reason"}),
	}
	for _, c := range []prometheus.Collector{m.received, m.rejected} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register server metric: %w", err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("POST /ingest", handleIngest(logger, queue, m, now, retryAfterHeader(retryAfter)))
	mux.Handle("GET /health", handleHealth(logger, db))
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return mux, nil
}

// handleIngest accepts a JSON array of events.
//
// A request is all-or-nothing: every event is validated first, then the whole
// array is enqueued atomically, so a client never has to work out which part
// of a request was stored. Responses:
//
//   - 202 with the number of accepted events;
//   - 400 when the body is not a single JSON array of events;
//   - 413 when the body exceeds 4 MiB or holds more than the buffer can ever fit;
//   - 422 when the array is empty or any event fails validation;
//   - 429 with Retry-After when the buffer lacks room for the whole array,
//     in events or in bytes;
//   - 503 while the service is shutting down.
//
// A 202 means the events are buffered, not yet written to ClickHouse.
func handleIngest(logger *slog.Logger, queue Enqueuer, m ingestMetrics, now func() time.Time, retryAfter string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, err := decodeEvents(w, r)
		if err != nil {
			status := http.StatusBadRequest
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			writeJSON(w, status, errorResponse{Error: err.Error()})
			return
		}

		if err := validate(events, now()); err != nil {
			m.rejected.WithLabelValues("invalid").Add(float64(len(events)))
			writeJSON(w, http.StatusUnprocessableEntity, errorResponse{Error: err.Error()})
			return
		}

		if err := queue.Enqueue(events); err != nil {
			status, reason := enqueueFailure(err)
			m.rejected.WithLabelValues(reason).Add(float64(len(events)))
			if status == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", retryAfter)
			}
			if status == http.StatusInternalServerError {
				logger.ErrorContext(r.Context(), "enqueue events", "error", err)
			}
			writeJSON(w, status, errorResponse{Error: err.Error()})
			return
		}

		m.received.Add(float64(len(events)))
		writeJSON(w, http.StatusAccepted, ingestResponse{Accepted: len(events)})
	})
}

func handleHealth(logger *slog.Logger, db Pinger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			logger.ErrorContext(ctx, "health check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	})
}

func decodeEvents(w http.ResponseWriter, r *http.Request) ([]event.Event, error) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()

	var events []event.Event
	if err := dec.Decode(&events); err != nil {
		return nil, fmt.Errorf("decode events: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errTrailingData
	}
	return events, nil
}

func validate(events []event.Event, now time.Time) error {
	if len(events) == 0 {
		return fmt.Errorf("%w: request contains no events", event.ErrInvalid)
	}
	for i := range events {
		if err := events[i].Validate(now); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
	}
	return nil
}

func enqueueFailure(err error) (status int, reason string) {
	switch {
	case errors.Is(err, batcher.ErrBufferFull):
		return http.StatusTooManyRequests, "buffer_full"
	case errors.Is(err, batcher.ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "too_large"
	case errors.Is(err, batcher.ErrClosed):
		return http.StatusServiceUnavailable, "shutting_down"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

// retryAfterHeader renders d as a Retry-After value in whole seconds.
func retryAfterHeader(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
