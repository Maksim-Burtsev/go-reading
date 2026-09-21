// Package status keeps the latest known state of every delivery.
package status

import (
	"maps"
	"sync"
	"time"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
)

// Record is the latest known state of a delivery.
type Record struct {
	ID        string          `json:"id"`
	URL       string          `json:"url"`
	Status    dispatch.Status `json:"status"`
	Attempts  int             `json:"attempts"`
	Error     string          `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Recorder holds delivery records in memory and evicts finished ones once
// they are older than the retention period.
type Recorder struct {
	retention time.Duration
	now       func() time.Time

	mu      sync.RWMutex
	records map[string]Record
}

// NewRecorder returns a Recorder that keeps finished records for retention.
func NewRecorder(retention time.Duration) *Recorder {
	return &Recorder{
		retention: retention,
		now:       time.Now,
		records:   make(map[string]Record),
	}
}

// Track stores t as a queued delivery and returns its record.
func (r *Recorder) Track(t dispatch.Task) Record {
	now := r.now()
	rec := Record{
		ID:        t.ID,
		URL:       t.URL,
		Status:    dispatch.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[t.ID] = rec
	return rec
}

// Forget drops the record for id.
func (r *Recorder) Forget(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
}

// Get returns the record for id and whether it exists.
func (r *Recorder) Get(id string) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[id]
	return rec, ok
}

// Run applies results until the channel is closed, evicting expired records
// along the way.
func (r *Recorder) Run(results <-chan dispatch.Result) {
	ticker := time.NewTicker(r.retention)
	defer ticker.Stop()

	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}
			r.apply(res)
		case <-ticker.C:
			r.evict()
		}
	}
}

func (r *Recorder) apply(res dispatch.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.records[res.TaskID]
	if !ok {
		return
	}
	rec.Status = res.Status
	rec.Attempts = res.Attempts
	rec.UpdatedAt = r.now()
	if res.Err != nil {
		rec.Error = res.Err.Error()
	}
	r.records[res.TaskID] = rec
}

func (r *Recorder) evict() {
	cutoff := r.now().Add(-r.retention)

	r.mu.Lock()
	defer r.mu.Unlock()
	maps.DeleteFunc(r.records, func(_ string, rec Record) bool {
		return rec.Status != dispatch.StatusQueued && rec.UpdatedAt.Before(cutoff)
	})
}
