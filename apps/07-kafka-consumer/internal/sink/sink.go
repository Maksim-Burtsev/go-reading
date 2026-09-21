// Package sink provides destinations for batches of events.
package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
)

// JSONLines writes events to an io.Writer, one JSON document per line.
type JSONLines struct {
	w io.Writer
}

// NewJSONLines returns a JSONLines sink writing to w.
func NewJSONLines(w io.Writer) *JSONLines {
	return &JSONLines{w: w}
}

// Write encodes the whole batch before issuing a single write to the
// underlying writer, so an encoding failure writes nothing.
func (s *JSONLines) Write(ctx context.Context, events []event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("encode event %s: %w", e.ID, err)
		}
	}
	if _, err := buf.WriteTo(s.w); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	return nil
}
