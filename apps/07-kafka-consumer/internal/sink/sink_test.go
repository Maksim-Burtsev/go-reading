package sink_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/event"
	"github.com/Maksim-Burtsev/go-reading/apps/07-kafka-consumer/internal/sink"
)

var errDiskFull = errors.New("disk full")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errDiskFull }

func TestJSONLinesWrite(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	events := []event.Event{
		{ID: "e-1", Type: "order.created", OccurredAt: at, Payload: []byte(`{"total":42}`)},
		{ID: "e-2", Type: "order.paid", Source: "billing", OccurredAt: at},
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		failing bool
		want    string
		wantErr error
	}{
		{
			name: "one line per event",
			ctx:  t.Context(),
			want: `{"id":"e-1","type":"order.created","occurred_at":"2026-09-01T10:00:00Z","payload":{"total":42}}` + "\n" +
				`{"id":"e-2","type":"order.paid","source":"billing","occurred_at":"2026-09-01T10:00:00Z"}` + "\n",
		},
		{name: "canceled context writes nothing", ctx: canceled, wantErr: context.Canceled},
		{name: "writer error", ctx: t.Context(), failing: true, wantErr: errDiskFull},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			s := sink.NewJSONLines(&buf)
			if tt.failing {
				s = sink.NewJSONLines(failingWriter{})
			}

			err := s.Write(tt.ctx, events)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, buf.String())
		})
	}
}
