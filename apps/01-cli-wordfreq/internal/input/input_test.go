package input

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		want    []string
		wantErr error
	}{
		{name: "no operands", args: nil, want: []string{Stdin}},
		{name: "files", args: []string{"a", "b"}, want: []string{"a", "b"}},
		{name: "stdin among files", args: []string{"a", Stdin, "b"}, want: []string{"a", Stdin, "b"}},
		{name: "stdin twice", args: []string{Stdin, "a", Stdin}, wantErr: ErrStdinRepeated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Names(tt.args)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOpenerOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "in.txt")
	require.NoError(t, os.WriteFile(path, []byte("from file"), 0o600))

	tests := []struct {
		name    string
		operand string
		want    string
		wantErr error
	}{
		{name: "file", operand: path, want: "from file"},
		{name: "stdin", operand: Stdin, want: "from stdin"},
		{name: "missing file", operand: filepath.Join(t.TempDir(), "missing.txt"), wantErr: fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := Opener{Stdin: strings.NewReader("from stdin")}
			rc, err := o.Open(t.Context(), tt.operand)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.Equal(t, tt.want, string(got))
		})
	}
}

type cancelingReader struct {
	r      io.Reader
	cancel context.CancelFunc
}

func (c cancelingReader) Read(p []byte) (int, error) {
	c.cancel()
	return c.r.Read(p)
}

func TestOpenerOpenCanceled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stdin func(cancel context.CancelFunc) io.Reader
	}{
		{
			name: "canceled before read",
			stdin: func(cancel context.CancelFunc) io.Reader {
				cancel()
				return strings.NewReader("unread")
			},
		},
		{
			name: "canceled during read",
			stdin: func(cancel context.CancelFunc) io.Reader {
				return cancelingReader{r: strings.NewReader(""), cancel: cancel}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rc, err := Opener{Stdin: tt.stdin(cancel)}.Open(ctx, Stdin)
			require.NoError(t, err)
			_, err = io.ReadAll(rc)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
