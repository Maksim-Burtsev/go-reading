package store

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidCursor is returned by DecodeCursor for a malformed token.
var ErrInvalidCursor = errors.New("invalid cursor")

// Cursor is a keyset position in a user's order list, which is sorted by
// creation time and then by ID, newest first.
type Cursor struct {
	CreatedAt time.Time
	ID        int64
}

// Encode returns the cursor as an opaque, URL-safe token.
func (c Cursor) Encode() string {
	raw := strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + ":" + strconv.FormatInt(c.ID, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses a token produced by Cursor.Encode.
func DecodeCursor(token string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}

	micros, id, ok := strings.Cut(string(raw), ":")
	if !ok {
		return Cursor{}, fmt.Errorf("%w: missing separator", ErrInvalidCursor)
	}

	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: timestamp: %w", ErrInvalidCursor, err)
	}

	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: id: %w", ErrInvalidCursor, err)
	}
	if n <= 0 {
		return Cursor{}, fmt.Errorf("%w: id must be positive", ErrInvalidCursor)
	}

	return Cursor{CreatedAt: time.UnixMicro(us), ID: n}, nil
}
