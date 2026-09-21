// Package backoff computes retry delays using capped exponential backoff with full jitter.
package backoff

import (
	"context"
	"time"
)

// Policy is a capped exponential backoff with full jitter.
type Policy struct {
	// Base is the delay ceiling for the first retry.
	Base time.Duration
	// Max caps the delay ceiling of every retry.
	Max time.Duration
	// Rand returns a uniformly distributed integer in [0, n). It must be safe
	// for concurrent use.
	Rand func(n int64) int64
}

// Delay returns the pause before the given retry, counted from zero: a random
// duration in [0, min(Max, Base*2^retry)).
func (p Policy) Delay(retry int) time.Duration {
	ceiling := p.Max
	if retry < 63 && p.Base <= p.Max>>retry {
		ceiling = p.Base << retry
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(p.Rand(int64(ceiling)))
}

// Sleep pauses for d or until ctx is done, whichever comes first. It returns
// the context's error when ctx ends the pause early.
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
