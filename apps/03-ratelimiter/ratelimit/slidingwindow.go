package ratelimit

import (
	"context"
	"time"
)

var _ Limiter = (*SlidingWindow)(nil)

// SlidingWindow is a Limiter implementing the sliding window counter algorithm. It counts the
// requests of every key in fixed windows aligned to the period and estimates the number of
// requests in the trailing period as the count of the current window plus the count of the
// previous window weighted by how much of it still overlaps the trailing period. Unlike a
// sliding log, it keeps two counters per key regardless of the limit.
type SlidingWindow struct {
	limit  int
	period time.Duration
	store  *store[window]
}

type window struct {
	start    time.Time
	current  int
	previous int
}

// NewSlidingWindow returns a SlidingWindow that allows rate.Limit requests per key in any
// trailing rate.Period. Close must be called to stop its background eviction.
func NewSlidingWindow(rate Rate, opts ...Option) (*SlidingWindow, error) {
	if err := rate.validate(); err != nil {
		return nil, err
	}

	s, err := newStore(2*rate.Period, func(time.Time) window { return window{} }, opts)
	if err != nil {
		return nil, err
	}
	return &SlidingWindow{limit: rate.Limit, period: rate.Period, store: s}, nil
}

// Allow counts a request for key if the estimated number of requests in the trailing period
// stays within the limit. It never blocks, so it ignores the context.
func (sw *SlidingWindow) Allow(_ context.Context, key string) (Decision, error) {
	return sw.store.do(key, sw.hit)
}

// Close stops the background eviction and drops all keys. Allow returns ErrClosed afterwards.
func (sw *SlidingWindow) Close() {
	sw.store.close()
}

func (sw *SlidingWindow) hit(w *window, now time.Time) Decision {
	sw.advance(w, now)

	d := Decision{Limit: sw.limit}
	if sw.estimate(*w, now)+1 <= float64(sw.limit) {
		w.current++
		d.Allowed = true
	} else {
		d.RetryAfter = sw.retryAfter(*w, now)
	}
	d.Remaining = max(0, int(float64(sw.limit)-sw.estimate(*w, now)))
	d.ResetAfter = sw.resetAfter(*w, now)
	return d
}

func (sw *SlidingWindow) advance(w *window, now time.Time) {
	start := now.Truncate(sw.period)
	switch start.Sub(w.start) {
	case 0:
		return
	case sw.period:
		w.previous, w.current = w.current, 0
	default:
		w.previous, w.current = 0, 0
	}
	w.start = start
}

func (sw *SlidingWindow) estimate(w window, now time.Time) float64 {
	overlap := w.start.Add(sw.period).Sub(now)
	return float64(w.previous)*float64(overlap)/float64(sw.period) + float64(w.current)
}

func (sw *SlidingWindow) retryAfter(w window, now time.Time) time.Duration {
	end := w.start.Add(sw.period)
	if w.current < sw.limit {
		return end.Add(-sw.maxOverlap(sw.limit-w.current-1, w.previous)).Sub(now)
	}
	return end.Add(sw.period - sw.maxOverlap(sw.limit-1, w.current)).Sub(now)
}

func (sw *SlidingWindow) maxOverlap(budget, count int) time.Duration {
	return time.Duration(float64(sw.period) * float64(budget) / float64(count))
}

func (sw *SlidingWindow) resetAfter(w window, now time.Time) time.Duration {
	end := w.start.Add(sw.period)
	switch {
	case w.current > 0:
		return end.Add(sw.period).Sub(now)
	case w.previous > 0:
		return end.Sub(now)
	default:
		return 0
	}
}
