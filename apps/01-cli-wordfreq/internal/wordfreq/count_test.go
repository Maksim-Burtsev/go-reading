package wordfreq

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

const brokenInput = "broken"

var errBroken = errors.New("broken reader")

type fakeOpener map[string]string

func (f fakeOpener) Open(_ context.Context, name string) (io.ReadCloser, error) {
	if name == brokenInput {
		return io.NopCloser(iotest.ErrReader(errBroken)), nil
	}
	text, ok := f[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(text)), nil
}

func TestCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		minLen int
		want   Counts
	}{
		{name: "empty", in: "", minLen: 1, want: Counts{}},
		{name: "case folded", in: "Hello hello HELLO, world", minLen: 1, want: Counts{"hello": 3, "world": 1}},
		{name: "cyrillic", in: "Мир мир МИР. Ёж ёж", minLen: 1, want: Counts{"мир": 3, "ёж": 2}},
		{name: "full case folding", in: "Straße STRASSE", minLen: 1, want: Counts{"strasse": 2}},
		{name: "min length in runes", in: "a an the ёж кот", minLen: 3, want: Counts{"the": 1, "кот": 1}},
		{name: "word over the default token size", in: strings.Repeat("ab", 50_000), minLen: 1, want: Counts{strings.Repeat("ab", 50_000): 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Count(strings.NewReader(tt.in), tt.minLen)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCountReadError(t *testing.T) {
	t.Parallel()
	_, err := Count(iotest.ErrReader(errBroken), 1)
	require.ErrorIs(t, err, errBroken)
}

func TestCountsTop(t *testing.T) {
	t.Parallel()

	counts := Counts{"b": 2, "a": 2, "c": 5, "d": 1}
	tests := []struct {
		name string
		n    int
		want []Entry
	}{
		{name: "all", n: 0, want: []Entry{{"c", 5}, {"a", 2}, {"b", 2}, {"d", 1}}},
		{name: "ties broken by word", n: 3, want: []Entry{{"c", 5}, {"a", 2}, {"b", 2}}},
		{name: "more than available", n: 10, want: []Entry{{"c", 5}, {"a", 2}, {"b", 2}, {"d", 1}}},
		{name: "one", n: 1, want: []Entry{{"c", 5}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, counts.Top(tt.n))
		})
	}
}

func TestCountsTopEmpty(t *testing.T) {
	t.Parallel()
	got := Counts{}.Top(10)
	require.NotNil(t, got)
	require.Empty(t, got)
}

func TestCountsMergeTotal(t *testing.T) {
	t.Parallel()
	c := Counts{"a": 1, "b": 2}
	c.Merge(Counts{"b": 3, "c": 4})
	require.Equal(t, Counts{"a": 1, "b": 5, "c": 4}, c)
	require.Equal(t, 10, c.Total())
}

func TestCountAll(t *testing.T) {
	t.Parallel()

	open := fakeOpener{
		"a": "the cat and the hat",
		"b": "The Cat sat",
		"c": "",
	}
	merged := Counts{"the": 3, "cat": 2, "and": 1, "hat": 1, "sat": 1}
	tests := []struct {
		name        string
		names       []string
		opts        Options
		want        Counts
		wantSkipped []string
		wantErr     error
	}{
		{name: "single input", names: []string{"b"}, opts: Options{MinLen: 1, Jobs: 1}, want: Counts{"the": 1, "cat": 1, "sat": 1}},
		{name: "sequential", names: []string{"a", "b", "c"}, opts: Options{MinLen: 1, Jobs: 1}, want: merged},
		{name: "concurrent", names: []string{"a", "b", "c"}, opts: Options{MinLen: 1, Jobs: 3}, want: merged},
		{name: "min length", names: []string{"a", "b"}, opts: Options{MinLen: 4, Jobs: 2}, want: Counts{}},
		{name: "zero options", names: []string{"a", "b", "c"}, opts: Options{}, want: merged},
		{name: "missing input", names: []string{"a", "missing"}, opts: Options{MinLen: 1, Jobs: 2}, wantErr: fs.ErrNotExist},
		{name: "read error", names: []string{"a", brokenInput}, opts: Options{MinLen: 1, Jobs: 2}, wantErr: errBroken},
		{
			name:        "keep going past missing input",
			names:       []string{"a", "missing", "b", "c"},
			opts:        Options{MinLen: 1, Jobs: 4, KeepGoing: true},
			want:        merged,
			wantSkipped: []string{"missing"},
		},
		{
			name:        "keep going past read error",
			names:       []string{brokenInput, "b"},
			opts:        Options{MinLen: 1, Jobs: 2, KeepGoing: true},
			want:        Counts{"the": 1, "cat": 1, "sat": 1},
			wantSkipped: []string{brokenInput},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, skipped, err := CountAll(t.Context(), open, tt.names, tt.opts)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			var gotSkipped []string
			for _, s := range skipped {
				gotSkipped = append(gotSkipped, s.Name)
			}
			require.Equal(t, tt.wantSkipped, gotSkipped)
		})
	}
}
