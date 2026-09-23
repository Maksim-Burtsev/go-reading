package ratelimit

import (
	"context"
	"fmt"
	"time"
)

var _ Limiter = (*TokenBucket)(nil)

// TokenBucket is a Limiter that gives every key a bucket holding up to burst tokens, refilled
// one token at a time at a constant rate. Each allowed request takes a token, so an idle key can
// spend its whole bucket at once and is then held to the refill rate.
type TokenBucket struct {
	burst    int
	interval time.Duration
	store    *store[bucket]
}

type bucket struct {
	tokens     int
	refilledAt time.Time
}

// NewTokenBucket returns a TokenBucket that adds rate.Limit tokens every rate.Period to buckets
// holding at most burst tokens. Close must be called to stop its background eviction.
func NewTokenBucket(rate Rate, burst int, opts ...Option) (*TokenBucket, error) {
	if err := rate.validate(); err != nil {
		return nil, err
	}
	if burst <= 0 {
		return nil, fmt.Errorf("%w: burst must be positive, got %d", ErrInvalidConfig, burst)
	}
	interval := rate.Period / time.Duration(rate.Limit)
	if interval <= 0 {
		return nil, fmt.Errorf("%w: %d per %s is more than one per nanosecond", ErrInvalidConfig, rate.Limit, rate.Period)
	}

	tb := &TokenBucket{burst: burst, interval: interval}
	s, err := newStore(time.Duration(burst)*interval, tb.newBucket, opts)
	if err != nil {
		return nil, err
	}
	tb.store = s
	return tb, nil
}

// Allow takes a token from the bucket of key if one is available. It never blocks, so it ignores
// the context.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Decision, error) {
	return tb.store.do(key, tb.take)
}

// Close stops the background eviction and drops all keys. Allow returns ErrClosed afterwards.
func (tb *TokenBucket) Close() {
	tb.store.close()
}

func (tb *TokenBucket) newBucket(now time.Time) bucket {
	return bucket{tokens: tb.burst, refilledAt: now}
}

func (tb *TokenBucket) take(b *bucket, now time.Time) Decision {
	tb.refill(b, now)

	d := Decision{Limit: tb.burst}
	if b.tokens > 0 {
		b.tokens--
		d.Allowed = true
	} else {
		d.RetryAfter = b.refilledAt.Add(tb.interval).Sub(now)
	}
	d.Remaining = b.tokens
	d.ResetAfter = b.refilledAt.Add(time.Duration(tb.burst-b.tokens) * tb.interval).Sub(now)
	return d
}

func (tb *TokenBucket) refill(b *bucket, now time.Time) {
	earned := int(now.Sub(b.refilledAt) / tb.interval)
	if earned >= tb.burst-b.tokens {
		b.tokens, b.refilledAt = tb.burst, now
		return
	}
	if earned > 0 {
		b.tokens += earned
		b.refilledAt = b.refilledAt.Add(time.Duration(earned) * tb.interval)
	}
}
