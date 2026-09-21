package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/cli"
	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/input"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestRun(t *testing.T) {
	t.Parallel()

	a := writeFile(t, "a.txt", "the cat and the hat\n")
	b := writeFile(t, "b.txt", "The Cat sat\n")
	lines1 := writeFile(t, "lines1.txt", "x\ny\nx\n")
	lines2 := writeFile(t, "lines2.txt", "y\nz\n")

	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		stdin      string
		wantOut    string
		wantJSON   string
		wantStderr string
		wantErr    error
	}{
		{
			name:  "count stdin",
			args:  []string{"count"},
			stdin: "b a b c b a",
			wantOut: "RANK  WORD  COUNT  SHARE\n" +
				"1     b     3      50.00%\n" +
				"2     a     2      33.33%\n" +
				"3     c     1      16.67%\n",
		},
		{
			name:     "count files as json",
			args:     []string{"count", "--top", "2", "--json", a, b},
			wantJSON: `{"total":8,"unique":5,"words":[{"word":"the","count":3},{"word":"cat","count":2}]}`,
		},
		{
			name:  "count files and stdin",
			args:  []string{"count", "--min-len", "3", "-j", "1", a, input.Stdin},
			stdin: "Кот и кот",
			wantOut: "RANK  WORD  COUNT  SHARE\n" +
				"1     the   2      28.57%\n" +
				"2     кот   2      28.57%\n" +
				"3     and   1      14.29%\n" +
				"4     cat   1      14.29%\n" +
				"5     hat   1      14.29%\n",
		},
		{
			name:     "count empty input",
			args:     []string{"count", "--json"},
			wantJSON: `{"total":0,"unique":0,"words":[]}`,
		},
		{
			name:    "dedupe stdin",
			args:    []string{"dedupe"},
			stdin:   "b\na\nb\nc\na\n",
			wantOut: "b\na\nc\n",
		},
		{
			name:    "dedupe files with counts",
			args:    []string{"dedupe", "--count", lines1, lines2},
			wantOut: "      2 x\n      2 y\n      1 z\n",
		},
		{
			name:       "log level from env",
			args:       []string{"dedupe"},
			env:        map[string]string{"WORDFREQ_LOG_LEVEL": "info"},
			stdin:      "q\nq\n",
			wantOut:    "q\n",
			wantStderr: `"msg":"dedupe finished","inputs":1,"lines":2,"unique":1`,
		},
		{
			name:    "missing file",
			args:    []string{"count", a, filepath.Join(t.TempDir(), "missing.txt")},
			wantErr: fs.ErrNotExist,
		},
		{
			name:    "stdin twice",
			args:    []string{"dedupe", input.Stdin, input.Stdin},
			wantErr: input.ErrStdinRepeated,
		},
		{
			name:    "zero jobs",
			args:    []string{"count", "--jobs", "0"},
			wantErr: cli.ErrInvalidFlag,
		},
		{
			name:    "negative top",
			args:    []string{"count", "--top", "-1"},
			wantErr: cli.ErrInvalidFlag,
		},
		{
			name:    "unknown log level",
			args:    []string{"count", "--log-level", "loud"},
			wantErr: cli.ErrInvalidFlag,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			getenv := func(key string) string { return tt.env[key] }
			args := append([]string{"wordfreq"}, tt.args...)

			err := run(t.Context(), args, getenv, strings.NewReader(tt.stdin), &stdout, &stderr)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantJSON != "" {
				require.JSONEq(t, tt.wantJSON, stdout.String())
			} else {
				require.Equal(t, tt.wantOut, stdout.String())
			}
			if tt.wantStderr != "" {
				require.Contains(t, stderr.String(), tt.wantStderr)
			} else {
				require.Empty(t, stderr.String())
			}
		})
	}
}

func TestRunEmptyArgs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), nil, os.Getenv, strings.NewReader(""), &stdout, &stderr)
	require.Error(t, err)
}
