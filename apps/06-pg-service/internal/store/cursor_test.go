package store_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cursor store.Cursor
	}{
		{"microsecond precision", store.Cursor{CreatedAt: time.Date(2026, 9, 21, 12, 30, 45, 123456000, time.UTC), ID: 42}},
		{"before unix epoch", store.Cursor{CreatedAt: time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC), ID: 1}},
		{"max id", store.Cursor{CreatedAt: time.Unix(0, 0), ID: 1<<63 - 1}},
		{"earliest timestamptz", store.Cursor{CreatedAt: time.Date(-4713, time.November, 24, 0, 0, 0, 0, time.UTC), ID: 1}},
		{"latest int64 microsecond", store.Cursor{CreatedAt: time.UnixMicro(1<<63 - 1), ID: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := store.DecodeCursor(tt.cursor.Encode())
			require.NoError(t, err)
			require.True(t, tt.cursor.CreatedAt.Equal(got.CreatedAt), "created_at: want %s, got %s", tt.cursor.CreatedAt, got.CreatedAt)
			require.Equal(t, tt.cursor.ID, got.ID)
		})
	}
}

func TestCursorEncodeTruncatesToMicroseconds(t *testing.T) {
	t.Parallel()

	c := store.Cursor{CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 999, time.UTC), ID: 7}

	got, err := store.DecodeCursor(c.Encode())
	require.NoError(t, err)
	require.True(t, got.CreatedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
}

func TestDecodeCursorRejectsMalformedTokens(t *testing.T) {
	t.Parallel()

	encode := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

	tests := []struct {
		name  string
		token string
	}{
		{"not base64", "!!!"},
		{"padded base64", base64.URLEncoding.EncodeToString([]byte("1:12"))},
		{"missing separator", encode("12345")},
		{"non-numeric timestamp", encode("yesterday:1")},
		{"non-numeric id", encode("1700000000000000:abc")},
		{"zero id", encode("1700000000000000:0")},
		{"negative id", encode("1700000000000000:-5")},
		{"id overflows int64", encode("1700000000000000:9223372036854775808")},
		{"timestamp before timestamptz range", encode("-210866803200000001:1")},
		{"minimum int64 timestamp", encode("-9223372036854775808:1")},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := store.DecodeCursor(tt.token)
			require.ErrorIs(t, err, store.ErrInvalidCursor)
		})
	}
}
