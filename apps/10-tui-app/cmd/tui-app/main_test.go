package main

import (
	"bytes"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
)

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		stdin      string
		wantErr    error
		wantErrMsg string
		wantID     string
		wantStatus task.Status
	}{
		{name: "quit without changes", stdin: "jjq", wantID: "PLAT-412", wantStatus: task.StatusInProgress},
		{name: "toggle and quit saves", stdin: "xq", wantID: "PLAT-412", wantStatus: task.StatusDone},
		{name: "ctrl+c quits and saves", stdin: "x\x03", wantID: "PLAT-412", wantStatus: task.StatusDone},
		{name: "q types into the filter", stdin: "/opaque\rxq", wantID: "API-1031", wantStatus: task.StatusDone},
		{name: "help flag", args: []string{"-h"}, wantID: "PLAT-412", wantStatus: task.StatusInProgress},
		{name: "unknown flag", args: []string{"-verbose"}, wantErrMsg: "parse flags"},
		{name: "missing file", args: []string{"-file", "missing.json"}, wantErr: fs.ErrNotExist},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "tasks.json")
			sample, err := task.Load(t.Context(), filepath.Join("..", "..", "tasks.json"))
			require.NoError(t, err)
			require.NoError(t, task.Save(t.Context(), path, sample))
			getenv := func(key string) string {
				if key == "TASKS_FILE" {
					return path
				}
				return ""
			}

			var stdout, stderr bytes.Buffer
			args := append([]string{"tui-app"}, tt.args...)
			err = run(t.Context(), args, getenv, strings.NewReader(tt.stdin), &stdout, &stderr)
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				return
			case tt.wantErrMsg != "":
				require.ErrorContains(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)

			tasks, err := task.Load(t.Context(), path)
			require.NoError(t, err)
			i := slices.IndexFunc(tasks, func(tk task.Task) bool { return tk.ID == tt.wantID })
			require.NotEqual(t, -1, i)
			require.Equal(t, tt.wantStatus, tasks[i].Status)
		})
	}
}
