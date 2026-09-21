// Package event defines the analytics event accepted by the sink.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	maxTypeLen   = 128
	maxUserIDLen = 256
)

// ErrInvalid is wrapped by every validation error returned from Validate.
var ErrInvalid = errors.New("invalid event")

// Event is a single analytics event as submitted by a client.
type Event struct {
	ID         uuid.UUID       `json:"event_id"`
	Type       string          `json:"event_type"`
	UserID     string          `json:"user_id"`
	Timestamp  time.Time       `json:"ts"`
	Properties json.RawMessage `json:"properties,omitempty"`
}

// Validate reports whether the event can be stored.
func (e *Event) Validate() error {
	switch {
	case e.ID == uuid.Nil:
		return fmt.Errorf("%w: event_id is required", ErrInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: event_type is required", ErrInvalid)
	case len(e.Type) > maxTypeLen:
		return fmt.Errorf("%w: event_type exceeds %d bytes", ErrInvalid, maxTypeLen)
	case e.UserID == "":
		return fmt.Errorf("%w: user_id is required", ErrInvalid)
	case len(e.UserID) > maxUserIDLen:
		return fmt.Errorf("%w: user_id exceeds %d bytes", ErrInvalid, maxUserIDLen)
	case e.Timestamp.IsZero():
		return fmt.Errorf("%w: ts is required", ErrInvalid)
	case len(e.Properties) > 0 && !e.hasObjectProperties():
		return fmt.Errorf("%w: properties must be a JSON object", ErrInvalid)
	}
	return nil
}

// PropertiesJSON returns the properties as a JSON object string, "{}" when absent.
func (e *Event) PropertiesJSON() string {
	if !e.hasObjectProperties() {
		return "{}"
	}
	return string(e.Properties)
}

func (e *Event) hasObjectProperties() bool {
	return bytes.HasPrefix(bytes.TrimSpace(e.Properties), []byte("{"))
}
