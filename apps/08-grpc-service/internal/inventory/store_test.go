package inventory_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/08-grpc-service/internal/inventory"
)

func newStore(t *testing.T, items ...inventory.Item) *inventory.Store {
	t.Helper()
	if len(items) == 0 {
		items = []inventory.Item{
			{SKU: "A-1", Name: "Alpha", Available: 5},
			{SKU: "A-2", Name: "Alpha two", Available: 0},
			{SKU: "B-1", Name: "Beta", Available: 2},
		}
	}
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	s, err := inventory.NewStore(items, func() time.Time { return now })
	require.NoError(t, err)
	return s
}

func available(t *testing.T, s *inventory.Store, sku string) int64 {
	t.Helper()
	item, err := s.Get(t.Context(), sku)
	require.NoError(t, err)
	return item.Available
}

func TestNewStoreRejectsInvalidItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		items []inventory.Item
	}{
		{name: "empty sku", items: []inventory.Item{{Available: 1}}},
		{name: "negative available", items: []inventory.Item{{SKU: "A", Available: -1}}},
		{name: "negative reserved", items: []inventory.Item{{SKU: "A", Reserved: -1}}},
		{name: "duplicate sku", items: []inventory.Item{{SKU: "A"}, {SKU: "A"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := inventory.NewStore(tt.items, time.Now)
			require.Error(t, err)
		})
	}
}

func TestStoreGet(t *testing.T) {
	t.Parallel()
	s := newStore(t)

	item, err := s.Get(t.Context(), "A-1")
	require.NoError(t, err)
	require.Equal(t, inventory.Item{SKU: "A-1", Name: "Alpha", Available: 5}, item)

	_, err = s.Get(t.Context(), "missing")
	require.ErrorIs(t, err, inventory.ErrNotFound)
}

func TestStoreList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter inventory.Filter
		want   []string
	}{
		{name: "all sorted", filter: inventory.Filter{}, want: []string{"A-1", "A-2", "B-1"}},
		{name: "prefix", filter: inventory.Filter{SKUPrefix: "A-"}, want: []string{"A-1", "A-2"}},
		{name: "in stock only", filter: inventory.Filter{InStockOnly: true}, want: []string{"A-1", "B-1"}},
		{name: "limit", filter: inventory.Filter{Limit: 2}, want: []string{"A-1", "A-2"}},
		{name: "limit above size", filter: inventory.Filter{Limit: 10}, want: []string{"A-1", "A-2", "B-1"}},
		{name: "no match", filter: inventory.Filter{SKUPrefix: "Z"}, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			items, err := newStore(t).List(t.Context(), tt.filter)
			require.NoError(t, err)
			got := make([]string, 0, len(items))
			for _, item := range items {
				got = append(got, item.SKU)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStoreReserve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		id            string
		lines         []inventory.Line
		wantErr       error
		wantLines     []inventory.Line
		wantAvailable map[string]int64
	}{
		{
			name:          "reserves every line sorted by sku",
			id:            "r1",
			lines:         []inventory.Line{{SKU: "B-1", Quantity: 2}, {SKU: "A-1", Quantity: 3}},
			wantLines:     []inventory.Line{{SKU: "A-1", Quantity: 3}, {SKU: "B-1", Quantity: 2}},
			wantAvailable: map[string]int64{"A-1": 2, "B-1": 0},
		},
		{name: "empty id", lines: []inventory.Line{{SKU: "A-1", Quantity: 1}}, wantErr: inventory.ErrInvalidReservation},
		{name: "id too long", id: strings.Repeat("x", 129), lines: []inventory.Line{{SKU: "A-1", Quantity: 1}}, wantErr: inventory.ErrInvalidReservation},
		{name: "no lines", id: "r1", wantErr: inventory.ErrInvalidReservation},
		{name: "zero quantity", id: "r1", lines: []inventory.Line{{SKU: "A-1"}}, wantErr: inventory.ErrInvalidReservation},
		{name: "negative quantity", id: "r1", lines: []inventory.Line{{SKU: "A-1", Quantity: -1}}, wantErr: inventory.ErrInvalidReservation},
		{name: "empty sku", id: "r1", lines: []inventory.Line{{Quantity: 1}}, wantErr: inventory.ErrInvalidReservation},
		{
			name:    "duplicate sku",
			id:      "r1",
			lines:   []inventory.Line{{SKU: "A-1", Quantity: 1}, {SKU: "A-1", Quantity: 1}},
			wantErr: inventory.ErrInvalidReservation,
		},
		{
			name:          "unknown sku reserves nothing",
			id:            "r1",
			lines:         []inventory.Line{{SKU: "A-1", Quantity: 1}, {SKU: "Z-9", Quantity: 1}},
			wantErr:       inventory.ErrNotFound,
			wantAvailable: map[string]int64{"A-1": 5},
		},
		{
			name:          "insufficient stock reserves nothing",
			id:            "r1",
			lines:         []inventory.Line{{SKU: "A-1", Quantity: 1}, {SKU: "B-1", Quantity: 3}},
			wantErr:       inventory.ErrInsufficientStock,
			wantAvailable: map[string]int64{"A-1": 5, "B-1": 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newStore(t)

			r, err := s.Reserve(t.Context(), tt.id, tt.lines)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.id, r.ID)
				require.Equal(t, tt.wantLines, r.Lines)
			}
			for sku, want := range tt.wantAvailable {
				require.Equal(t, want, available(t, s, sku), sku)
			}
		})
	}
}

func TestStoreReserveIsIdempotent(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	lines := []inventory.Line{{SKU: "A-1", Quantity: 2}, {SKU: "B-1", Quantity: 1}}

	first, err := s.Reserve(t.Context(), "r1", lines)
	require.NoError(t, err)

	replay, err := s.Reserve(t.Context(), "r1", []inventory.Line{lines[1], lines[0]})
	require.NoError(t, err)
	require.Equal(t, first, replay)
	require.Equal(t, int64(3), available(t, s, "A-1"))

	_, err = s.Reserve(t.Context(), "r1", []inventory.Line{{SKU: "A-1", Quantity: 1}})
	require.ErrorIs(t, err, inventory.ErrReservationConflict)

	item, err := s.Get(t.Context(), "A-1")
	require.NoError(t, err)
	require.Equal(t, int64(2), item.Reserved)
}

func TestStoreReserveHonoursContext(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := s.Reserve(ctx, "r1", []inventory.Line{{SKU: "A-1", Quantity: 1}})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(5), available(t, s, "A-1"))
}

func TestStoreConcurrentReservations(t *testing.T) {
	t.Parallel()

	const (
		stock   = 25
		callers = 100
	)
	tests := []struct {
		name         string
		id           func(i int) string
		wantOK       int
		wantReserved int64
	}{
		{name: "distinct ids share stock", id: func(i int) string { return fmt.Sprintf("r%d", i) }, wantOK: stock, wantReserved: stock},
		{name: "same id reserves once", id: func(int) string { return "r" }, wantOK: callers, wantReserved: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newStore(t, inventory.Item{SKU: "A-1", Available: stock})

			var (
				mu   sync.Mutex
				ok   int
				errs []error
				wg   sync.WaitGroup
			)
			for i := range callers {
				wg.Go(func() {
					_, err := s.Reserve(t.Context(), tt.id(i), []inventory.Line{{SKU: "A-1", Quantity: 1}})
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						errs = append(errs, err)
						return
					}
					ok++
				})
			}
			wg.Wait()

			require.Equal(t, tt.wantOK, ok)
			for _, err := range errs {
				require.ErrorIs(t, err, inventory.ErrInsufficientStock)
			}
			item, err := s.Get(t.Context(), "A-1")
			require.NoError(t, err)
			require.Equal(t, tt.wantReserved, item.Reserved)
			require.Equal(t, stock-tt.wantReserved, item.Available)
		})
	}
}
