// Package input maps command-line operands to readers.
package input

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
)

// Stdin is the operand that stands for standard input.
const Stdin = "-"

// ErrStdinRepeated is returned when standard input is named more than once.
var ErrStdinRepeated = errors.New("standard input (-) given more than once")

// Names returns the operands to read: args itself, or standard input alone
// when args is empty.
func Names(args []string) ([]string, error) {
	if len(args) == 0 {
		return []string{Stdin}, nil
	}
	if i := slices.Index(args, Stdin); i >= 0 && slices.Contains(args[i+1:], Stdin) {
		return nil, ErrStdinRepeated
	}
	return args, nil
}

// Opener opens operands by name.
type Opener struct {
	Stdin io.Reader
}

// Open returns a reader for the named file, or for o.Stdin when name is
// Stdin. Reads fail with ctx's error once ctx is done. Closing the result
// never closes o.Stdin.
func (o Opener) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == Stdin {
		return io.NopCloser(&contextReader{ctx: ctx, r: o.Stdin}), nil
	}
	f, err := os.Open(name) //nolint:gosec // G304: reading the files the user names is the point
	if err != nil {
		return nil, err
	}
	return readCloser{Reader: &contextReader{ctx: ctx, r: f}, Closer: f}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}
