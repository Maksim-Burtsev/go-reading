package notes

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore() *Store {
	s := NewStore()
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(time.Second)
		return now
	}
	return s
}

func TestStoreCreateGet(t *testing.T) {
	t.Parallel()

	s := newTestStore()
	created := s.Create(Input{Title: "groceries", Body: "milk", Tags: []string{"home"}})

	require.NotEmpty(t, created.ID)
	require.Equal(t, "groceries", created.Title)
	require.Equal(t, created.CreatedAt, created.UpdatedAt)

	got, err := s.Get(created.ID)
	require.NoError(t, err)
	require.Equal(t, created, got)
}

func TestStoreUpdate(t *testing.T) {
	t.Parallel()

	s := newTestStore()
	created := s.Create(Input{Title: "draft", Tags: []string{"a"}})

	updated, err := s.Update(created.ID, Input{Title: "final", Body: "done"})
	require.NoError(t, err)
	require.Equal(t, created.ID, updated.ID)
	require.Equal(t, "final", updated.Title)
	require.Equal(t, "done", updated.Body)
	require.Empty(t, updated.Tags)
	require.Equal(t, created.CreatedAt, updated.CreatedAt)
	require.True(t, updated.UpdatedAt.After(created.UpdatedAt))

	got, err := s.Get(created.ID)
	require.NoError(t, err)
	require.Equal(t, updated, got)
}

func TestStoreDelete(t *testing.T) {
	t.Parallel()

	s := newTestStore()
	created := s.Create(Input{Title: "temp"})

	require.NoError(t, s.Delete(created.ID))

	_, err := s.Get(created.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, s.List())
}

func TestStoreNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   func(s *Store) error
	}{
		{
			name: "get",
			op: func(s *Store) error {
				_, err := s.Get("missing")
				return err
			},
		},
		{
			name: "update",
			op: func(s *Store) error {
				_, err := s.Update("missing", Input{Title: "x"})
				return err
			},
		},
		{
			name: "delete",
			op:   func(s *Store) error { return s.Delete("missing") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.op(newTestStore())
			require.ErrorIs(t, err, ErrNotFound)
			require.ErrorContains(t, err, `"missing"`)
		})
	}
}

func TestStoreListOrdersByCreation(t *testing.T) {
	t.Parallel()

	s := newTestStore()
	var want []string
	for i := range 5 {
		want = append(want, s.Create(Input{Title: fmt.Sprintf("note %d", i)}).ID)
	}

	var got []string
	for _, n := range s.List() {
		got = append(got, n.ID)
	}
	require.Equal(t, want, got)
}

func TestStoreListByTag(t *testing.T) {
	t.Parallel()

	s := newTestStore()
	a := s.Create(Input{Title: "a", Tags: []string{"home"}})
	b := s.Create(Input{Title: "b", Tags: []string{"home", "work"}})
	c := s.Create(Input{Title: "c", Tags: []string{"work"}})

	ids := func(ns []Note) []string {
		out := make([]string, 0, len(ns))
		for _, n := range ns {
			out = append(out, n.ID)
		}
		return out
	}

	require.Equal(t, []string{a.ID, b.ID}, ids(s.ListByTag("home", 0)))
	require.Equal(t, []string{b.ID, c.ID}, ids(s.ListByTag("work", 0)))
	require.Empty(t, s.ListByTag("missing", 0))
	require.Len(t, s.ListByTag("work", 1), 1)

	require.NoError(t, s.Delete(b.ID))
	require.Equal(t, []string{a.ID}, ids(s.ListByTag("home", 0)))
	require.Equal(t, []string{c.ID}, ids(s.ListByTag("work", 0)))
}

func TestStoreDoesNotShareTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(t *testing.T, s *Store, id string, input []string)
	}{
		{
			name:   "input slice",
			mutate: func(_ *testing.T, _ *Store, _ string, input []string) { input[0] = "mutated" },
		},
		{
			name: "get result",
			mutate: func(t *testing.T, s *Store, id string, _ []string) {
				n, err := s.Get(id)
				require.NoError(t, err)
				n.Tags[0] = "mutated"
			},
		},
		{
			name: "list result",
			mutate: func(_ *testing.T, s *Store, _ string, _ []string) {
				s.List()[0].Tags[0] = "mutated"
			},
		},
		{
			name: "update result",
			mutate: func(t *testing.T, s *Store, id string, _ []string) {
				n, err := s.Update(id, Input{Title: "t", Tags: []string{"original"}})
				require.NoError(t, err)
				n.Tags[0] = "mutated"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore()
			input := []string{"original"}
			created := s.Create(Input{Title: "t", Tags: input})

			tt.mutate(t, s, created.ID, input)

			got, err := s.Get(created.ID)
			require.NoError(t, err)
			require.Equal(t, []string{"original"}, got.Tags)
		})
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	t.Parallel()

	const workers = 32
	s := NewStore()

	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			n := s.Create(Input{Title: fmt.Sprintf("note %d", i), Tags: []string{"shared"}})
			_, err := s.Update(n.ID, Input{Title: "updated", Tags: []string{"changed"}})
			assert.NoError(t, err)
			_, err = s.Get(n.ID)
			assert.NoError(t, err)
			_ = s.List()
			if i%2 == 0 {
				assert.NoError(t, s.Delete(n.ID))
			}
		})
	}
	wg.Wait()

	all := s.List()
	require.Len(t, all, workers/2)
	for _, n := range all {
		require.Equal(t, "updated", n.Title)
		require.Equal(t, []string{"changed"}, n.Tags)
	}
}
