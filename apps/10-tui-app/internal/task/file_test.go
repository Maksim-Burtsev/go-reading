package task

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		file      string
		wantErr   error
		wantCount int
	}{
		{name: "valid", file: "valid.json", wantCount: 3},
		{name: "missing file", file: "missing.json", wantErr: fs.ErrNotExist},
		{name: "malformed json", file: "malformed.json", wantErr: ErrMalformed},
		{name: "unknown field", file: "unknown_field.json", wantErr: ErrMalformed},
		{name: "trailing data", file: "trailing.json", wantErr: ErrMalformed},
		{name: "bad due date", file: "bad_date.json", wantErr: ErrMalformed},
		{name: "invalid fields", file: "invalid.json", wantErr: ErrInvalidTask},
		{name: "duplicate id", file: "duplicate.json", wantErr: ErrDuplicateID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tasks, err := Load(t.Context(), filepath.Join("testdata", tt.file))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, tasks)
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks, tt.wantCount)
		})
	}
}

func TestLoadValidFields(t *testing.T) {
	t.Parallel()

	tasks, err := Load(t.Context(), filepath.Join("testdata", "valid.json"))
	require.NoError(t, err)

	require.Equal(t, Task{
		ID:          "API-1031",
		Title:       "Paginate GET /v2/invoices with an opaque cursor",
		Status:      StatusTodo,
		Priority:    PriorityHigh,
		Tags:        []string{"api", "billing"},
		Due:         Date{time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)},
		Description: "Replace offset pagination with a keyset cursor.",
	}, tasks[0])
	require.True(t, tasks[1].Due.IsZero())
	require.Nil(t, tasks[2].Tags)
}

func TestLoadReportsEveryProblem(t *testing.T) {
	t.Parallel()

	_, err := Load(t.Context(), filepath.Join("testdata", "invalid.json"))
	require.ErrorContains(t, err, "task #1: invalid task: empty id")
	require.ErrorContains(t, err, `task "API-2": invalid task: empty title`)
	require.ErrorContains(t, err, `unknown status "blocked"`)
	require.ErrorContains(t, err, `unknown priority "urgent"`)
}

func TestLoadCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Load(ctx, filepath.Join("testdata", "valid.json"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestSaveRoundTrip(t *testing.T) {
	t.Parallel()

	tasks, err := Load(t.Context(), filepath.Join("testdata", "valid.json"))
	require.NoError(t, err)
	tasks[0].Status = StatusDone

	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.json")
	require.NoError(t, Save(t.Context(), path, tasks))

	got, err := Load(t.Context(), path)
	require.NoError(t, err)
	require.Equal(t, tasks, got)

	raw, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"due": "2026-09-30"`)
	require.Equal(t, 1, strings.Count(string(raw), `"due"`))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
