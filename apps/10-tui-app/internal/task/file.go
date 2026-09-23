package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Errors returned by Load.
var (
	ErrMalformed   = errors.New("malformed task file")
	ErrInvalidTask = errors.New("invalid task")
	ErrDuplicateID = errors.New("duplicate task id")
)

type document struct {
	Tasks []Task `json:"tasks"`
}

// Load reads and validates the task file at path. Validation problems are
// reported together, each wrapping ErrInvalidTask or ErrDuplicateID.
func Load(ctx context.Context, path string) ([]Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // G304: reading the task file the user names is the point
	if err != nil {
		return nil, fmt.Errorf("read task file: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var doc document
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: unexpected data after the top-level object", ErrMalformed)
	}

	if err := validate(doc.Tasks); err != nil {
		return nil, err
	}
	return doc.Tasks, nil
}

func validate(tasks []Task) error {
	var errs []error
	seen := make(map[string]struct{}, len(tasks))
	for i, t := range tasks {
		ref := fmt.Sprintf("task %q", t.ID)
		_, dup := seen[t.ID]
		switch {
		case strings.TrimSpace(t.ID) == "":
			ref = fmt.Sprintf("task #%d", i+1)
			errs = append(errs, fmt.Errorf("%s: %w: empty id", ref, ErrInvalidTask))
		case dup:
			errs = append(errs, fmt.Errorf("%s: %w", ref, ErrDuplicateID))
		}
		if strings.TrimSpace(t.Title) == "" {
			errs = append(errs, fmt.Errorf("%s: %w: empty title", ref, ErrInvalidTask))
		}
		if !t.Status.Valid() {
			errs = append(errs, fmt.Errorf("%s: %w: unknown status %q", ref, ErrInvalidTask, t.Status))
		}
		if !t.Priority.Valid() {
			errs = append(errs, fmt.Errorf("%s: %w: unknown priority %q", ref, ErrInvalidTask, t.Priority))
		}
		seen[t.ID] = struct{}{}
	}
	return errors.Join(errs...)
}

// Save atomically replaces the task file at path with tasks. A symlink at
// path is followed: the file it points to is replaced and the link stays.
// The new file keeps the permission bits of the one it replaces.
func Save(ctx context.Context, path string, tasks []Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(document{Tasks: tasks}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode tasks: %w", err)
	}
	target, perm, err := replaceTarget(path)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(target), "."+filepath.Base(target)+".*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod %s: %w", tmp.Name(), err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmp.Name(), err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return fmt.Errorf("replace %s: %w", target, err)
	}
	return nil
}

// replaceTarget returns the file Save replaces, path itself or the file a
// symlink at path points to, and the permission bits for its replacement:
// the current file's, or 0600 for a file that does not exist yet.
func replaceTarget(path string) (string, fs.FileMode, error) {
	target, err := filepath.EvalSymlinks(path)
	if errors.Is(err, fs.ErrNotExist) {
		return path, 0o600, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("resolve %s: %w", path, err)
	}
	info, err := os.Stat(target)
	if err != nil {
		return "", 0, fmt.Errorf("stat %s: %w", target, err)
	}
	return target, info.Mode().Perm(), nil
}
