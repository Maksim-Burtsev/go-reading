// Package retry runs operations with capped exponential backoff and jitter.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// ErrExhausted is returned by Do when every attempt failed.
var ErrExhausted = errors.New("retry attempts exhausted")

// Policy describes how many times an operation is attempted and how long to
// wait between attempts.
type Policy struct {
	attempts  int
	baseDelay time.Duration
	maxDelay  time.Duration
	jitter    func(ceiling time.Duration) time.Duration
	sleep     func(ctx context.Context, d time.Duration) error
}

// New returns a Policy that makes up to attempts calls. The wait before the
// n-th retry is drawn uniformly from [0, min(maxDelay, baseDelay*2^(n-1))).
func New(attempts int, baseDelay, maxDelay time.Duration) Policy {
	return Policy{
		attempts:  max(attempts, 1),
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		jitter:    rand.N[time.Duration],
		sleep:     sleep,
	}
}

// Do calls op until it succeeds, the attempts run out or ctx is done. The
// attempt number passed to op starts at 1. When all attempts fail, the
// returned error wraps both ErrExhausted and the last error from op.
func (p Policy) Do(ctx context.Context, op func(ctx context.Context, attempt int) error) error {
	var err error
	for attempt := range p.attempts {
		if attempt > 0 {
			if serr := p.sleep(ctx, p.backoff(attempt)); serr != nil {
				return fmt.Errorf("wait before attempt %d: %w (last error: %w)", attempt+1, serr, err)
			}
		}
		if err = op(ctx, attempt+1); err == nil {
			return nil
		}
	}
	return fmt.Errorf("%w after %d attempts: %w", ErrExhausted, p.attempts, err)
}

func (p Policy) backoff(retry int) time.Duration {
	ceiling := p.maxDelay
	if shift := retry - 1; shift < 63 && p.baseDelay <= p.maxDelay>>shift {
		ceiling = p.baseDelay << shift
	}
	if ceiling <= 0 {
		return 0
	}
	return p.jitter(ceiling)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
