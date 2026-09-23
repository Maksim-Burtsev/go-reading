// Package event defines the domain events carried on the input topic.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrInvalid is returned by Decode for payloads that are not valid events.
var ErrInvalid = errors.New("invalid event")

// The occurred_at values the events table holds: ClickHouse's DateTime64 starts
// in 1900, and the driver converts a time through int64 nanoseconds since 1970,
// which run out in April 2262.
var (
	minOccurredAt = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxOccurredAt = time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// Event is a domain event published by upstream services. ID identifies the
// event across redeliveries.
type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Source     string          `json:"source,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Decode parses a JSON-encoded event and checks its required fields and that
// occurred_at lies in [1900-01-01, 2262-01-01).
func Decode(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	switch {
	case e.ID == "":
		return Event{}, fmt.Errorf("%w: missing id", ErrInvalid)
	case e.Type == "":
		return Event{}, fmt.Errorf("%w: missing type", ErrInvalid)
	case e.OccurredAt.IsZero():
		return Event{}, fmt.Errorf("%w: missing occurred_at", ErrInvalid)
	case e.OccurredAt.Before(minOccurredAt) || !e.OccurredAt.Before(maxOccurredAt):
		return Event{}, fmt.Errorf("%w: occurred_at is outside [%s, %s)",
			ErrInvalid, minOccurredAt.Format(time.DateOnly), maxOccurredAt.Format(time.DateOnly))
	}
	return e, nil
}
