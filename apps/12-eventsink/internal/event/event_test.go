package event_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/12-eventsink/internal/event"
)

func TestDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		want    event.Event
		wantErr string
	}{
		{
			name: "full event",
			data: `{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-21T10:00:00.5Z","payload":{"total":42}}`,
			want: event.Event{
				ID:         "e-1",
				Type:       "order.created",
				Source:     "checkout",
				OccurredAt: time.Date(2026, 9, 21, 10, 0, 0, 500_000_000, time.UTC),
				Payload:    json.RawMessage(`{"total":42}`),
			},
		},
		{
			name: "optional fields omitted",
			data: `{"id":"e-2","type":"order.paid","occurred_at":"2026-09-21T10:00:00Z"}`,
			want: event.Event{ID: "e-2", Type: "order.paid", OccurredAt: time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)},
		},
		{name: "not json", data: `not json`, wantErr: "invalid character"},
		{name: "missing id", data: `{"type":"order.paid","occurred_at":"2026-09-21T10:00:00Z"}`, wantErr: "missing id"},
		{name: "missing type", data: `{"id":"e-3","occurred_at":"2026-09-21T10:00:00Z"}`, wantErr: "missing type"},
		{name: "missing occurred_at", data: `{"id":"e-4","type":"order.paid"}`, wantErr: "missing occurred_at"},
		{name: "malformed occurred_at", data: `{"id":"e-5","type":"order.paid","occurred_at":"yesterday"}`, wantErr: "cannot parse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := event.Decode([]byte(tt.data))
			if tt.wantErr != "" {
				require.ErrorIs(t, err, event.ErrInvalid)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
