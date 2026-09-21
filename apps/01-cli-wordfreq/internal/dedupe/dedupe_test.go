package dedupe

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeOpener map[string]string

func (f fakeOpener) Open(_ context.Context, name string) (io.ReadCloser, error) {
	text, ok := f[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(text)), nil
}

func TestSetScanAll(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("я", 100_000)
	tests := []struct {
		name      string
		inputs    fakeOpener
		names     []string
		wantFirst []string
		wantLines []Line
		wantErr   error
	}{
		{
			name:      "first-seen order",
			inputs:    fakeOpener{"a": "b\na\nb\nc\na\n"},
			names:     []string{"a"},
			wantFirst: []string{"b", "a", "c"},
			wantLines: []Line{{"b", 2}, {"a", 2}, {"c", 1}},
		},
		{
			name:      "crlf and no final newline",
			inputs:    fakeOpener{"a": "x\r\ny\nx"},
			names:     []string{"a"},
			wantFirst: []string{"x", "y"},
			wantLines: []Line{{"x", 2}, {"y", 1}},
		},
		{
			name:      "empty lines",
			inputs:    fakeOpener{"a": "\n\nz\n"},
			names:     []string{"a"},
			wantFirst: []string{"", "z"},
			wantLines: []Line{{"", 2}, {"z", 1}},
		},
		{
			name:      "across inputs",
			inputs:    fakeOpener{"a": "1\n2\n", "b": "2\n3\n"},
			names:     []string{"a", "b"},
			wantFirst: []string{"1", "2", "3"},
			wantLines: []Line{{"1", 1}, {"2", 2}, {"3", 1}},
		},
		{
			name:      "line longer than default scanner buffer",
			inputs:    fakeOpener{"a": long + "\n" + long + "\n"},
			names:     []string{"a"},
			wantFirst: []string{long},
			wantLines: []Line{{long, 2}},
		},
		{
			name:    "missing input",
			inputs:  fakeOpener{"a": "1\n"},
			names:   []string{"a", "missing"},
			wantErr: fs.ErrNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			set := NewSet()
			var first []string
			err := set.ScanAll(t.Context(), tt.inputs, tt.names, func(line string) error {
				first = append(first, line)
				return nil
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantFirst, first)
			require.Equal(t, tt.wantLines, set.Lines())
			require.Equal(t, len(tt.wantLines), set.Len())
		})
	}
}

func TestSetScanAllWithoutCallback(t *testing.T) {
	t.Parallel()
	set := NewSet()
	require.NoError(t, set.ScanAll(t.Context(), fakeOpener{"a": "q\nq\nw\n"}, []string{"a"}, nil))
	require.Equal(t, []Line{{"q", 2}, {"w", 1}}, set.Lines())
	require.Equal(t, 3, set.Total())
}

func TestSetScanAllLineTooLong(t *testing.T) {
	t.Parallel()
	inputs := fakeOpener{"a": strings.Repeat("x", maxLineSize+1)}
	err := NewSet().ScanAll(t.Context(), inputs, []string{"a"}, nil)
	require.ErrorIs(t, err, bufio.ErrTooLong)
}

func TestSetScanAllCallbackError(t *testing.T) {
	t.Parallel()
	errStop := errors.New("stop")
	set := NewSet()
	err := set.ScanAll(t.Context(), fakeOpener{"a": "1\n2\n3\n"}, []string{"a"}, func(string) error {
		return errStop
	})
	require.ErrorIs(t, err, errStop)
	require.Equal(t, 1, set.Total())
}
