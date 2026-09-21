package dispatch

import "sync"

// Queue is a bounded in-memory FIFO of tasks that never blocks producers.
type Queue struct {
	mu     sync.RWMutex
	closed bool
	tasks  chan Task
}

// NewQueue returns a Queue that holds up to size pending tasks.
func NewQueue(size int) *Queue {
	return &Queue{tasks: make(chan Task, size)}
}

// Push enqueues t. It returns ErrQueueFull when the queue is at capacity and
// ErrQueueClosed once Close has been called.
func (q *Queue) Push(t Task) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return ErrQueueClosed
	}
	select {
	case q.tasks <- t:
		return nil
	default:
		return ErrQueueFull
	}
}

// Tasks returns the channel consumers receive tasks from. It is closed by
// Close after the tasks already queued.
func (q *Queue) Tasks() <-chan Task {
	return q.tasks
}

// Len reports the number of queued tasks.
func (q *Queue) Len() int {
	return len(q.tasks)
}

// Close stops the queue from accepting tasks. It is safe to call more than once.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	close(q.tasks)
}
