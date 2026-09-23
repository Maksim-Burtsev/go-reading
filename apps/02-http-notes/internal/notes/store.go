// Package notes defines the note model and an in-memory store for it.
package notes

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrNotFound is returned when no note has the requested ID.
var ErrNotFound = errors.New("note not found")

// Note is a stored note.
type Note struct {
	ID        string
	Title     string
	Body      string
	Tags      []string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Input holds the user-editable fields of a note.
type Input struct {
	Title string
	Body  string
	Tags  []string
}

// Store is an in-memory note store that is safe for concurrent use.
// Notes are copied on the way in and on the way out, so callers never share
// memory with the store.
type Store struct {
	mu    sync.RWMutex
	notes map[string]Note
	byTag map[string]map[string]struct{}
	now   func() time.Time
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		notes: make(map[string]Note),
		byTag: make(map[string]map[string]struct{}),
		now:   func() time.Time { return time.Now().UTC() },
	}
}

// Create stores a new note built from in and returns it.
func (s *Store) Create(in Input) Note {
	now := s.now()
	n := Note{
		ID:        uuid.NewString(),
		Title:     in.Title,
		Body:      in.Body,
		Tags:      slices.Clone(in.Tags),
		CreatedAt: now,
		UpdatedAt: now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.notes[n.ID] = n
	s.index(n.ID, n.Tags)
	return n.clone()
}

// Get returns the note with the given ID.
func (s *Store) Get(id string) (Note, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.notes[id]
	if !ok {
		return Note{}, fmt.Errorf("get note %q: %w", id, ErrNotFound)
	}
	return n.clone(), nil
}

// List returns all notes ordered by creation time, oldest first.
func (s *Store) List() []Note {
	s.mu.RLock()
	out := make([]Note, 0, len(s.notes))
	for _, n := range s.notes {
		out = append(out, n.clone())
	}
	s.mu.RUnlock()

	slices.SortFunc(out, compareNotes)
	return out
}

// ListByTag returns the notes that carry tag, ordered like List. A positive
// limit caps the number of notes returned.
func (s *Store) ListByTag(tag string, limit int) []Note {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.byTag[tag]
	out := make([]Note, 0, len(ids))
	for id := range ids {
		if limit > 0 && len(out) == limit {
			break
		}
		n, err := s.Get(id)
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	slices.SortFunc(out, compareNotes)
	return out
}

// Update replaces the editable fields of the note with the given ID.
func (s *Store) Update(id string, in Input) (Note, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.notes[id]
	if !ok {
		return Note{}, fmt.Errorf("update note %q: %w", id, ErrNotFound)
	}
	n.Title = in.Title
	n.Body = in.Body
	n.Tags = slices.Clone(in.Tags)
	n.UpdatedAt = s.now()
	s.notes[id] = n
	s.index(id, n.Tags)
	return n.clone(), nil
}

// Delete removes the note with the given ID.
func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.notes[id]; !ok {
		return fmt.Errorf("delete note %q: %w", id, ErrNotFound)
	}
	delete(s.notes, id)
	s.unindex(id)
	return nil
}

func (s *Store) index(id string, tags []string) {
	for _, tag := range tags {
		if s.byTag[tag] == nil {
			s.byTag[tag] = make(map[string]struct{})
		}
		s.byTag[tag][id] = struct{}{}
	}
}

func (s *Store) unindex(id string) {
	for tag, ids := range s.byTag {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.byTag, tag)
		}
	}
}

func compareNotes(a, b Note) int {
	return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), strings.Compare(a.ID, b.ID))
}

func (n Note) clone() Note {
	n.Tags = slices.Clone(n.Tags)
	return n
}
