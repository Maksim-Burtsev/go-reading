package warehouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRejectsDSNWithoutLeakingPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{name: "not a URL", dsn: "clickhouse://eventsink:s3cret@localhost:9o12/eventsink", wantErr: "not a valid URL"},
		{name: "malformed setting", dsn: "clickhouse://eventsink:s3cret@localhost:9012/eventsink?dial_timeout=soon", wantErr: "dial timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Open(t.Context(), tt.dsn)
			require.ErrorContains(t, err, tt.wantErr)
			require.NotContains(t, err.Error(), "s3cret")
		})
	}
}
