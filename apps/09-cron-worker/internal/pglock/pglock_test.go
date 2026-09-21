package pglock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want int64
	}{
		{name: "purge-expired-sessions", want: 6529178692433963126},
		{name: "rollup-daily-events", want: 5474144793353804291},
		{name: "expire-pending-orders", want: 5825987350685310730},
		{name: "", want: 5472609002491880229},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, Key(tt.name))
		})
	}
}
