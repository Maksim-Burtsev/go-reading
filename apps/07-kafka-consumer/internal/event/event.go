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

// Event is a domain event published by upstream services.
type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Source     string          `json:"source,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Decode parses a JSON-encoded event and checks its required fields.
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
	}
	return e, nil
}
