// Package inventory holds stock levels and reservations for warehouse items.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	maxReservationIDLen = 128
	maxReservationLines = 100
)

var (
	// ErrNotFound is returned when a SKU is unknown.
	ErrNotFound = errors.New("item not found")
	// ErrInsufficientStock is returned when a reservation asks for more than is available.
	ErrInsufficientStock = errors.New("insufficient stock")
	// ErrReservationConflict is returned when a reservation ID is reused with different lines.
	ErrReservationConflict = errors.New("reservation id already used with different lines")
	// ErrInvalidReservation is returned when a reservation request is malformed.
	ErrInvalidReservation = errors.New("invalid reservation")
)

// Item is a stock-keeping unit and its current stock levels.
type Item struct {
	SKU       string
	Name      string
	Available int64
	Reserved  int64
}

// Line is a quantity of a single SKU within a reservation.
type Line struct {
	SKU      string
	Quantity int64
}

// Reservation is a committed hold on stock.
type Reservation struct {
	ID        string
	Lines     []Line
	CreatedAt time.Time
}

// Filter narrows the items returned by List. A zero Limit means no limit.
type Filter struct {
	SKUPrefix   string
	InStockOnly bool
	Limit       int
}

// Store is an in-memory inventory. It is safe for concurrent use.
type Store struct {
	now func() time.Time

	mu           sync.Mutex
	items        map[string]Item
	reservations map[string]Reservation
}

// NewStore returns a store holding the given items. The now function stamps new reservations.
func NewStore(items []Item, now func() time.Time) (*Store, error) {
	bySKU := make(map[string]Item, len(items))
	for _, item := range items {
		if item.SKU == "" {
			return nil, errors.New("item with empty sku")
		}
		if item.Available < 0 || item.Reserved < 0 {
			return nil, fmt.Errorf("item %s: negative stock", item.SKU)
		}
		if _, ok := bySKU[item.SKU]; ok {
			return nil, fmt.Errorf("item %s: duplicate sku", item.SKU)
		}
		bySKU[item.SKU] = item
	}
	return &Store{
		now:          now,
		items:        bySKU,
		reservations: make(map[string]Reservation),
	}, nil
}

// Get returns the item with the given SKU.
func (s *Store) Get(ctx context.Context, sku string) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Item{}, fmt.Errorf("get %s: %w", sku, err)
	}
	item, ok := s.items[sku]
	if !ok {
		return Item{}, fmt.Errorf("get %s: %w", sku, ErrNotFound)
	}
	return item, nil
}

// List returns a snapshot of the items matching f, sorted by SKU.
func (s *Store) List(ctx context.Context, f Filter) ([]Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	items := make([]Item, 0, len(s.items))
	for _, item := range s.items {
		if !strings.HasPrefix(item.SKU, f.SKUPrefix) {
			continue
		}
		if f.InStockOnly && item.Available == 0 {
			continue
		}
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b Item) int { return strings.Compare(a.SKU, b.SKU) })
	if f.Limit > 0 {
		items = items[:min(f.Limit, len(items))]
	}
	return items, nil
}

// Reserve moves the requested quantities from available to reserved stock, all
// lines or none. Repeating a call with the same id and lines returns the
// original reservation; the same id with different lines fails with
// ErrReservationConflict.
func (s *Store) Reserve(ctx context.Context, id string, lines []Line) (Reservation, error) {
	lines, err := normalize(id, lines)
	if err != nil {
		return Reservation{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Reservation{}, fmt.Errorf("reserve %s: %w", id, err)
	}
	if existing, ok := s.reservations[id]; ok {
		if !slices.Equal(existing.Lines, lines) {
			return Reservation{}, fmt.Errorf("reserve %s: %w", id, ErrReservationConflict)
		}
		return existing.clone(), nil
	}
	for _, line := range lines {
		item, ok := s.items[line.SKU]
		if !ok {
			return Reservation{}, fmt.Errorf("reserve %s: sku %s: %w", id, line.SKU, ErrNotFound)
		}
		if item.Available < line.Quantity {
			return Reservation{}, fmt.Errorf("reserve %s: sku %s: requested %d, available %d: %w",
				id, line.SKU, line.Quantity, item.Available, ErrInsufficientStock)
		}
	}
	for _, line := range lines {
		item := s.items[line.SKU]
		item.Available -= line.Quantity
		item.Reserved += line.Quantity
		s.items[line.SKU] = item
	}

	r := Reservation{ID: id, Lines: lines, CreatedAt: s.now()}
	s.reservations[id] = r
	return r.clone(), nil
}

func (r Reservation) clone() Reservation {
	r.Lines = slices.Clone(r.Lines)
	return r
}

func normalize(id string, lines []Line) ([]Line, error) {
	switch {
	case id == "":
		return nil, fmt.Errorf("%w: empty id", ErrInvalidReservation)
	case len(id) > maxReservationIDLen:
		return nil, fmt.Errorf("%w: id longer than %d bytes", ErrInvalidReservation, maxReservationIDLen)
	case len(lines) == 0:
		return nil, fmt.Errorf("%w: no lines", ErrInvalidReservation)
	case len(lines) > maxReservationLines:
		return nil, fmt.Errorf("%w: more than %d lines", ErrInvalidReservation, maxReservationLines)
	}

	sorted := slices.Clone(lines)
	slices.SortFunc(sorted, func(a, b Line) int { return strings.Compare(a.SKU, b.SKU) })
	for i, line := range sorted {
		if line.SKU == "" {
			return nil, fmt.Errorf("%w: line with empty sku", ErrInvalidReservation)
		}
		if line.Quantity <= 0 {
			return nil, fmt.Errorf("%w: sku %s: quantity must be positive", ErrInvalidReservation, line.SKU)
		}
		if i > 0 && sorted[i-1].SKU == line.SKU {
			return nil, fmt.Errorf("%w: sku %s: duplicate line", ErrInvalidReservation, line.SKU)
		}
	}
	return sorted, nil
}
