// Package dedupe removes repeated lines while keeping the order in which
// lines are first seen.
package dedupe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
)

const maxLineSize = 16 << 20

// Line is a distinct line and the number of times it occurred.
type Line struct {
	Text  string
	Count int
}

// Opener opens a named input whose reads fail once ctx is done.
type Opener interface {
	Open(ctx context.Context, name string) (io.ReadCloser, error)
}

// Set records distinct lines in first-seen order. Use NewSet to create one.
type Set struct {
	index map[string]int
	lines []Line
	total int
}

// NewSet returns an empty Set.
func NewSet() *Set {
	return &Set{index: make(map[string]int)}
}

// Add records one occurrence of text and reports whether it is the first.
func (s *Set) Add(text string) bool {
	s.total++
	if i, ok := s.index[text]; ok {
		s.lines[i].Count++
		return false
	}
	s.index[text] = len(s.lines)
	s.lines = append(s.lines, Line{Text: text, Count: 1})
	return true
}

// Lines returns the distinct lines with their counts in first-seen order.
func (s *Set) Lines() []Line {
	return slices.Clone(s.lines)
}

// Len returns the number of distinct lines.
func (s *Set) Len() int {
	return len(s.lines)
}

// Total returns the number of lines added, repeats included.
func (s *Set) Total() int {
	return s.total
}

// ScanAll adds every line of the named inputs, in order, to s. Each line
// seen for the first time is passed to onFirst as soon as it is read, unless
// onFirst is nil. Line terminators ("\n" or "\r\n") are not part of a line.
func (s *Set) ScanAll(ctx context.Context, open Opener, names []string, onFirst func(string) error) error {
	for _, name := range names {
		if err := s.scanInput(ctx, open, name, onFirst); err != nil {
			return err
		}
	}
	return nil
}

func (s *Set) scanInput(ctx context.Context, open Opener, name string, onFirst func(string) error) (err error) {
	rc, err := open.Open(ctx, name)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, rc.Close())
	}()

	if err := s.scan(rc, onFirst); err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	return nil
}

func (s *Set) scan(r io.Reader, onFirst func(string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineSize)
	for sc.Scan() {
		line := sc.Text()
		if !s.Add(line) || onFirst == nil {
			continue
		}
		if err := onFirst(line); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan lines: %w", err)
	}
	return nil
}
