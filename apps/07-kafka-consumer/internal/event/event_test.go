package event_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
)

func TestDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		want    event.Event
		wantErr bool
	}{
		{
			name: "valid event",
			data: `{"id":"e-1","type":"order.created","source":"checkout","occurred_at":"2026-09-01T10:00:00Z","payload":{"total":42}}`,
			want: event.Event{
				ID:         "e-1",
				Type:       "order.created",
				Source:     "checkout",
				OccurredAt: time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC),
				Payload:    []byte(`{"total":42}`),
			},
		},
		{name: "not json", data: `order created`, wantErr: true},
		{name: "empty value", data: ``, wantErr: true},
		{name: "json array", data: `[1,2]`, wantErr: true},
		{name: "missing id", data: `{"type":"order.created","occurred_at":"2026-09-01T10:00:00Z"}`, wantErr: true},
		{name: "missing type", data: `{"id":"e-1","occurred_at":"2026-09-01T10:00:00Z"}`, wantErr: true},
		{name: "missing occurred_at", data: `{"id":"e-1","type":"order.created"}`, wantErr: true},
		{name: "malformed timestamp", data: `{"id":"e-1","type":"order.created","occurred_at":"yesterday"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := event.Decode([]byte(tt.data))
			if tt.wantErr {
				require.ErrorIs(t, err, event.ErrInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
