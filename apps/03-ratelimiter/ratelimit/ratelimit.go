// Package ratelimit provides in-memory, per-key rate limiters and an HTTP middleware that
// enforces them.
//
// TokenBucket allows bursts up to a fixed capacity, and SlidingWindow smooths the limit over a
// trailing period. Both evict idle keys in the background and must be closed after use.
package ratelimit

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidConfig is returned by constructors when a parameter is out of range.
	ErrInvalidConfig = errors.New("ratelimit: invalid config")
	// ErrClosed is returned by Allow after the limiter has been closed.
	ErrClosed = errors.New("ratelimit: limiter closed")
)

// Decision is the outcome of a single Allow call.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the largest number of requests the key can make at once.
	Limit int
	// Remaining is the number of requests the key can still make right now.
	Remaining int
	// ResetAfter is how long until the key is back to its full limit.
	ResetAfter time.Duration
	// RetryAfter is how long until the next request can be allowed. It is zero when Allowed is true.
	RetryAfter time.Duration
}

// Rate is a number of requests allowed per period.
type Rate struct {
	Limit  int
	Period time.Duration
}

func (r Rate) validate() error {
	if r.Limit <= 0 {
		return fmt.Errorf("%w: limit must be positive, got %d", ErrInvalidConfig, r.Limit)
	}
	if r.Period <= 0 {
		return fmt.Errorf("%w: period must be positive, got %s", ErrInvalidConfig, r.Period)
	}
	return nil
}

// Option configures a limiter.
type Option func(*options)

type options struct {
	clock           Clock
	cleanupInterval time.Duration
}

// WithClock sets the time source of the limiter. The default is SystemClock.
func WithClock(c Clock) Option {
	return func(o *options) { o.clock = c }
}

// WithCleanupInterval sets how often idle keys are evicted. The default is one minute.
func WithCleanupInterval(d time.Duration) Option {
	return func(o *options) { o.cleanupInterval = d }
}

// Clock is the time source of a limiter.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers the time on its channel at regular intervals, like time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// SystemClock is a Clock backed by the time package.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// NewTicker returns a Ticker backed by time.NewTicker.
func (SystemClock) NewTicker(d time.Duration) Ticker { return systemTicker{time.NewTicker(d)} }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time { return s.t.C }

func (s systemTicker) Stop() { s.t.Stop() }
