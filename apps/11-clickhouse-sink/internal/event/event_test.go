package event_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/11-clickhouse-sink/internal/event"
)

func validEvent() event.Event {
	return event.Event{
		ID:         uuid.MustParse("0b5e4a1c-2f4e-4c55-9d2b-4f6a3c1e8a01"),
		Type:       "page_view",
		UserID:     "u-1",
		Timestamp:  time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC),
		Properties: json.RawMessage(`{"path":"/"}`),
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		mutate  func(e *event.Event)
		wantErr string
	}{
		{name: "valid", mutate: func(*event.Event) {}},
		{name: "no properties", mutate: func(e *event.Event) { e.Properties = nil }},
		{name: "missing id", mutate: func(e *event.Event) { e.ID = uuid.Nil }, wantErr: "event_id is required"},
		{name: "missing type", mutate: func(e *event.Event) { e.Type = "" }, wantErr: "event_type is required"},
		{
			name:    "type too long",
			mutate:  func(e *event.Event) { e.Type = strings.Repeat("x", 129) },
			wantErr: "event_type exceeds 128 bytes",
		},
		{name: "missing user", mutate: func(e *event.Event) { e.UserID = "" }, wantErr: "user_id is required"},
		{
			name:    "user too long",
			mutate:  func(e *event.Event) { e.UserID = strings.Repeat("u", 257) },
			wantErr: "user_id exceeds 256 bytes",
		},
		{name: "missing ts", mutate: func(e *event.Event) { e.Timestamp = time.Time{} }, wantErr: "ts is required"},
		{name: "ts 30 days old", mutate: func(e *event.Event) { e.Timestamp = now.AddDate(0, 0, -30) }},
		{
			name:    "ts too old",
			mutate:  func(e *event.Event) { e.Timestamp = now.AddDate(0, 0, -30).Add(-time.Millisecond) },
			wantErr: "ts is more than 30 days old",
		},
		{
			name:    "ts too far ahead",
			mutate:  func(e *event.Event) { e.Timestamp = now.Add(25 * time.Hour) },
			wantErr: "ts is more than a day ahead",
		},
		{
			name:    "array properties",
			mutate:  func(e *event.Event) { e.Properties = json.RawMessage(`[1,2]`) },
			wantErr: "properties must be a JSON object",
		},
		{
			name:    "null properties",
			mutate:  func(e *event.Event) { e.Properties = json.RawMessage(`null`) },
			wantErr: "properties must be a JSON object",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := validEvent()
			tt.mutate(&e)

			err := e.Validate(now)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, event.ErrInvalid)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestPropertiesJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		properties json.RawMessage
		want       string
	}{
		{name: "object", properties: json.RawMessage(`{"a":1}`), want: `{"a":1}`},
		{name: "absent", properties: nil, want: "{}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := validEvent()
			e.Properties = tt.properties
			require.Equal(t, tt.want, e.PropertiesJSON())
		})
	}
}
