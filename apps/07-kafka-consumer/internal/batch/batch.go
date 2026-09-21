// Package batch accumulates items until a size or age limit is reached.
package batch

import "time"

// Batcher collects items and reports when they are due for a flush: once it
// is full or once its oldest item has waited for the timeout.
//
// A Batcher is not safe for concurrent use.
type Batcher[T any] struct {
	size     int
	timeout  time.Duration
	items    []T
	deadline time.Time
}

// New returns a Batcher that flushes at size items or after timeout.
func New[T any](size int, timeout time.Duration) *Batcher[T] {
	return &Batcher[T]{
		size:    size,
		timeout: timeout,
		items:   make([]T, 0, size),
	}
}

// Add appends an item received at now and reports whether the batch is full.
func (b *Batcher[T]) Add(now time.Time, item T) bool {
	if len(b.items) == 0 {
		b.deadline = now.Add(b.timeout)
	}
	b.items = append(b.items, item)
	return len(b.items) >= b.size
}

// Len returns the number of pending items.
func (b *Batcher[T]) Len() int {
	return len(b.items)
}

// Room returns how many items can be added before the batch is full.
func (b *Batcher[T]) Room() int {
	return max(b.size-len(b.items), 0)
}

// Deadline returns the time at which the pending items time out. The second
// result is false when the batch is empty.
func (b *Batcher[T]) Deadline() (time.Time, bool) {
	return b.deadline, len(b.items) > 0
}

// Due reports whether the batch should be flushed at now.
func (b *Batcher[T]) Due(now time.Time) bool {
	if len(b.items) == 0 {
		return false
	}
	return len(b.items) >= b.size || !now.Before(b.deadline)
}

// Drain returns the pending items and leaves the batch empty.
func (b *Batcher[T]) Drain() []T {
	items := b.items
	b.items = make([]T, 0, b.size)
	b.deadline = time.Time{}
	return items
}
