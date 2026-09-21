// Package dispatch queues webhook deliveries and performs them with a bounded
// pool of workers.
package dispatch

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	// ErrQueueFull is returned by Queue.Push when the queue is at capacity.
	ErrQueueFull = errors.New("queue is full")
	// ErrQueueClosed is returned by Queue.Push after Queue.Close.
	ErrQueueClosed = errors.New("queue is closed")
	// ErrPermanent marks a delivery failure that retrying cannot fix.
	ErrPermanent = errors.New("permanent delivery failure")
	// ErrRetriesExhausted marks a delivery that failed on every allowed attempt.
	ErrRetriesExhausted = errors.New("retries exhausted")
)

// Task is a webhook delivery: a JSON payload to POST to URL.
type Task struct {
	ID      string
	URL     string
	Payload []byte
}

// Status is the lifecycle state of a delivery.
type Status string

// Delivery states.
const (
	StatusQueued    Status = "queued"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

// Result is the final outcome of a Task.
type Result struct {
	TaskID   string
	Status   Status
	Attempts int
	Err      error
}

// StatusError reports a non-2xx response from a delivery target.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("target responded %d %s", e.Code, http.StatusText(e.Code))
}
