// Package wordfreq counts case-folded word frequencies across many inputs.
package wordfreq

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/cases"
)

const maxWordSize = 16 << 20

// Counts maps a case-folded word to the number of its occurrences.
type Counts map[string]int

// Entry is a word and the number of its occurrences.
type Entry struct {
	Word  string `json:"word"`
	Count int    `json:"count"`
}

// Merge adds every count in other to c.
func (c Counts) Merge(other Counts) {
	for word, n := range other {
		c[word] += n
	}
}

// Total returns the number of word occurrences in c.
func (c Counts) Total() int {
	total := 0
	for _, n := range c {
		total += n
	}
	return total
}

// Top returns the n most frequent words, ordered by count descending and then
// by word ascending. A non-positive n returns every word.
func (c Counts) Top(n int) []Entry {
	entries := make([]Entry, 0, len(c))
	for word, count := range c {
		entries = append(entries, Entry{Word: word, Count: count})
	}
	slices.SortFunc(entries, func(a, b Entry) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), strings.Compare(a.Word, b.Word))
	})
	if n > 0 {
		entries = entries[:min(n, len(entries))]
	}
	return entries
}

// Count tallies the words read from r. Words are case-folded before they are
// counted, and folded words shorter than minLen runes are skipped.
func Count(r io.Reader, minLen int) (Counts, error) {
	fold := cases.Fold()
	counts := make(Counts)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxWordSize)
	sc.Split(ScanWords)
	for sc.Scan() {
		word := fold.String(sc.Text())
		if utf8.RuneCountInString(word) < minLen {
			continue
		}
		counts[word]++
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan words: %w", err)
	}
	return counts, nil
}

// Opener opens a named input whose reads fail once ctx is done.
type Opener interface {
	Open(ctx context.Context, name string) (io.ReadCloser, error)
}

// Options controls CountAll.
type Options struct {
	// MinLen is the minimum length of a counted word, in runes.
	MinLen int
	// Jobs is the maximum number of inputs read at the same time. Values
	// below 1 mean runtime.GOMAXPROCS(0).
	Jobs int
}

// CountAll counts the words of every named input, reading up to opts.Jobs
// inputs concurrently. The first failure cancels the remaining reads and is
// returned.
func CountAll(ctx context.Context, open Opener, names []string, opts Options) (Counts, error) {
	jobs := opts.Jobs
	if jobs < 1 {
		jobs = runtime.GOMAXPROCS(0)
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(jobs)

	var mu sync.Mutex
	total := make(Counts)
	for _, name := range names {
		g.Go(func() error {
			counts, err := countInput(ctx, open, name, opts.MinLen)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			total.Merge(counts)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return total, nil
}

func countInput(ctx context.Context, open Opener, name string, minLen int) (_ Counts, err error) {
	rc, err := open.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rc.Close())
	}()

	counts, err := Count(rc, minLen)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return counts, nil
}
