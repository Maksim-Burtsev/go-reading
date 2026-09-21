// Package task defines the task model and reads and writes task files.
package task

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

// Known task statuses.
const (
	StatusTodo       Status = "todo"
	StatusInProgress Status = "in_progress"
	StatusDone       Status = "done"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusTodo, StatusInProgress, StatusDone:
		return true
	default:
		return false
	}
}

// Priority is the urgency of a task.
type Priority string

// Known task priorities.
const (
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
)

// Valid reports whether p is a known priority.
func (p Priority) Valid() bool {
	switch p {
	case PriorityLow, PriorityMedium, PriorityHigh:
		return true
	default:
		return false
	}
}

// Date is a calendar date encoded in JSON as YYYY-MM-DD.
type Date struct {
	time.Time
}

// MarshalJSON implements json.Marshaler.
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Format(time.DateOnly))
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Date) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("date must be a string: %w", err)
	}
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return fmt.Errorf("parse date %q: %w", s, err)
	}
	d.Time = t
	return nil
}

// Task is a single unit of work.
type Task struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Status      Status   `json:"status"`
	Priority    Priority `json:"priority"`
	Tags        []string `json:"tags,omitempty"`
	Due         Date     `json:"due,omitzero"`
	Description string   `json:"description,omitempty"`
}

// Matches reports whether query is a case-insensitive substring of the title
// or of any tag. An empty query matches every task.
func (t Task) Matches(query string) bool {
	q := strings.ToLower(query)
	if strings.Contains(strings.ToLower(t.Title), q) {
		return true
	}
	return slices.ContainsFunc(t.Tags, func(tag string) bool {
		return strings.Contains(strings.ToLower(tag), q)
	})
}

// Overdue reports whether the task is still open and its due date is before
// the calendar day of now.
func (t Task) Overdue(now time.Time) bool {
	if t.Status == StatusDone || t.Due.IsZero() {
		return false
	}
	year, month, day := now.Date()
	return t.Due.Before(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}
