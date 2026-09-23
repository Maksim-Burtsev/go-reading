// Package lru provides a generic, concurrency-safe least-recently-used cache
// with optional entry expiry.
package lru

import (
	"container/list"
	"errors"
	"iter"
	"strconv"
	"sync"
	"time"
)

// ErrInvalidCapacity is returned by New when the capacity is not positive.
var ErrInvalidCapacity = errors.New("lru: capacity must be positive")

// EvictReason describes why an entry left the cache.
type EvictReason int

const (
	// ReasonCapacity means the entry was the least recently used one when a
	// new entry needed room.
	ReasonCapacity EvictReason = iota + 1
	// ReasonExpired means the entry's TTL had elapsed.
	ReasonExpired
	// ReasonRemoved means the entry was deleted by Remove or Purge.
	ReasonRemoved
)

// String returns the lower-case name of the reason.
func (r EvictReason) String() string {
	switch r {
	case ReasonCapacity:
		return "capacity"
	case ReasonExpired:
		return "expired"
	case ReasonRemoved:
		return "removed"
	default:
		return "EvictReason(" + strconv.Itoa(int(r)) + ")"
	}
}

// Stats is a point-in-time snapshot of the cache counters.
type Stats struct {
	// Hits counts Get calls that found a live entry.
	Hits uint64
	// Misses counts Get calls that found nothing or an expired entry.
	Misses uint64
	// Evictions counts entries dropped to make room for new ones.
	Evictions uint64
	// Expirations counts entries removed because their TTL had elapsed.
	Expirations uint64
}

// HitRatio returns Hits / (Hits + Misses), or 0 if there were no lookups.
func (s Stats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Option configures a Cache.
type Option[K comparable, V any] func(*Cache[K, V])

// WithTTL makes every entry expire d after it was last set. Expired entries
// are removed lazily on access or by DeleteExpired. A non-positive d disables
// expiry, which is the default.
func WithTTL[K comparable, V any](d time.Duration) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.ttl = d
	}
}

// WithOnEvict registers fn to be called for every entry that leaves the cache
// because of capacity pressure, expiry, Remove or Purge. Replacing the value
// of an existing key does not call fn.
//
// fn runs synchronously on the goroutine that caused the eviction, after the
// cache lock has been released, so it may call back into the cache. Callbacks
// triggered by concurrent operations may run in any order.
func WithOnEvict[K comparable, V any](fn func(key K, value V, reason EvictReason)) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.onEvict = fn
	}
}

// WithClock replaces time.Now as the source of the current time.
func WithClock[K comparable, V any](now func() time.Time) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.now = now
	}
}

type entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

func (e *entry[K, V]) expiredAt(now time.Time) bool {
	return !now.Before(e.expiresAt)
}

type eviction[K comparable, V any] struct {
	key    K
	value  V
	reason EvictReason
}

// Cache is a fixed-capacity LRU cache. The zero value is not usable; create
// instances with New. All methods are safe for concurrent use.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	now      func() time.Time
	onEvict  func(K, V, EvictReason)
	order    *list.List
	items    map[K]*list.Element
	stats    Stats
}

// New returns a cache holding at most capacity entries.
func New[K comparable, V any](capacity int, opts ...Option[K, V]) (*Cache[K, V], error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	c := &Cache[K, V]{
		capacity: capacity,
		now:      time.Now,
		order:    list.New(),
		items:    make(map[K]*list.Element, capacity),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Get returns the value stored under key and marks it as most recently used.
// An expired entry is removed and reported as a miss.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	value, ok, evicted := c.getLocked(key)
	c.mu.Unlock()
	c.notify(evicted)
	return value, ok
}

// Peek returns the value stored under key without updating its recency or
// the hit and miss counters. An expired entry is reported as absent.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		if e := el.Value.(*entry[K, V]); !c.expiredLocked(e) {
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

// Set stores value under key, marks it as most recently used and resets its
// TTL. If the cache is full, the least recently used entry is evicted.
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	evicted := c.setLocked(key, value)
	c.mu.Unlock()
	c.notify(evicted)
}

// Remove deletes the entry stored under key and reports whether it existed.
func (c *Cache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	el, ok := c.items[key]
	var evicted []eviction[K, V]
	if ok {
		evicted = c.evictLocked(el, ReasonRemoved, nil)
	}
	c.mu.Unlock()
	c.notify(evicted)
	return ok
}

// Purge deletes every entry.
func (c *Cache[K, V]) Purge() {
	c.mu.Lock()
	var evicted []eviction[K, V]
	for el := c.order.Back(); el != nil; el = c.order.Back() {
		evicted = c.evictLocked(el, ReasonRemoved, evicted)
	}
	c.mu.Unlock()
	c.notify(evicted)
}

// DeleteExpired removes every expired entry and returns how many were removed.
func (c *Cache[K, V]) DeleteExpired() int {
	if c.ttl <= 0 {
		return 0
	}
	c.mu.Lock()
	now := c.now()
	var (
		evicted []eviction[K, V]
		removed int
	)
	for el := c.order.Back(); el != nil; {
		prev := el.Prev()
		if el.Value.(*entry[K, V]).expiredAt(now) {
			evicted = c.evictLocked(el, ReasonExpired, evicted)
			removed++
		}
		el = prev
	}
	c.mu.Unlock()
	c.notify(evicted)
	return removed
}

// All returns an iterator over the live entries, from the least to the most
// recently used. Expired entries are skipped. Iterating does not update
// recency or the counters.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		now := c.now()
		for key, el := range c.items {
			e := el.Value.(*entry[K, V])
			if c.ttl > 0 && e.expiredAt(now) {
				continue
			}
			if !yield(key, e.value) {
				return
			}
		}
	}
}

// Len returns the number of entries, including expired entries that have not
// been removed yet.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Stats returns a snapshot of the cache counters.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Cache[K, V]) getLocked(key K) (V, bool, []eviction[K, V]) {
	var zero V
	el, ok := c.items[key]
	if !ok {
		c.stats.Misses++
		return zero, false, nil
	}
	e := el.Value.(*entry[K, V])
	if c.expiredLocked(e) {
		c.stats.Misses++
		return zero, false, c.evictLocked(el, ReasonExpired, nil)
	}
	c.order.MoveToFront(el)
	c.stats.Hits++
	return e.value, true, nil
}

func (c *Cache[K, V]) setLocked(key K, value V) []eviction[K, V] {
	var expiresAt time.Time
	if c.ttl > 0 {
		expiresAt = c.now().Add(c.ttl)
	}
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry[K, V])
		e.value, e.expiresAt = value, expiresAt
		c.order.MoveToFront(el)
		return nil
	}
	c.items[key] = c.order.PushFront(&entry[K, V]{key: key, value: value, expiresAt: expiresAt})
	if c.order.Len() <= c.capacity {
		return nil
	}
	oldest := c.order.Back()
	reason := ReasonCapacity
	if c.expiredLocked(oldest.Value.(*entry[K, V])) {
		reason = ReasonExpired
	}
	return c.evictLocked(oldest, reason, nil)
}

func (c *Cache[K, V]) evictLocked(el *list.Element, reason EvictReason, evicted []eviction[K, V]) []eviction[K, V] {
	e := c.order.Remove(el).(*entry[K, V])
	delete(c.items, e.key)
	switch reason {
	case ReasonCapacity:
		c.stats.Evictions++
	case ReasonExpired:
		c.stats.Expirations++
	}
	if c.onEvict == nil {
		return evicted
	}
	return append(evicted, eviction[K, V]{key: e.key, value: e.value, reason: reason})
}

func (c *Cache[K, V]) expiredLocked(e *entry[K, V]) bool {
	return c.ttl > 0 && e.expiredAt(c.now())
}

func (c *Cache[K, V]) notify(evicted []eviction[K, V]) {
	for _, ev := range evicted {
		c.onEvict(ev.key, ev.value, ev.reason)
	}
}
