//go:build integration

package store_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/db"
	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

func TestStore(t *testing.T) {
	ctx := t.Context()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("orders"),
		postgres.WithUsername("orders"),
		postgres.WithPassword("orders"),
		postgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	applied, err := db.Migrate(ctx, pool)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, applied)

	applied, err = db.Migrate(ctx, pool)
	require.NoError(t, err)
	require.Empty(t, applied)

	s := store.New(pool)

	t.Run("users", func(t *testing.T) {
		t.Parallel()
		testUsers(t, s)
	})
	t.Run("create order", func(t *testing.T) {
		t.Parallel()
		testCreateOrder(t, s, pool)
	})
	t.Run("list orders", func(t *testing.T) {
		t.Parallel()
		testListOrders(t, s)
	})
}

func testUsers(t *testing.T, s *store.Store) {
	ctx := t.Context()

	user, err := s.CreateUser(ctx, "ann@example.com", "Ann")
	require.NoError(t, err)
	require.Positive(t, user.ID)
	require.False(t, user.CreatedAt.IsZero())

	got, err := s.GetUser(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, user.Email, got.Email)
	require.True(t, user.CreatedAt.Equal(got.CreatedAt))

	_, err = s.CreateUser(ctx, "ann@example.com", "Another Ann")
	require.ErrorIs(t, err, store.ErrEmailTaken)

	_, err = s.GetUser(ctx, user.ID+1_000_000)
	require.ErrorIs(t, err, store.ErrUserNotFound)
}

func testCreateOrder(t *testing.T, s *store.Store, pool *pgxpool.Pool) {
	user, err := s.CreateUser(t.Context(), "orders@example.com", "Buyer")
	require.NoError(t, err)

	tests := []struct {
		name      string
		userID    int64
		items     []store.Item
		wantErr   error
		wantTotal int64
	}{
		{
			name:   "total is computed from items",
			userID: user.ID,
			items: []store.Item{
				{SKU: "BOOK-1", Quantity: 2, UnitPriceCents: 1999},
				{SKU: "PEN-7", Quantity: 3, UnitPriceCents: 150},
			},
			wantTotal: 2*1999 + 3*150,
		},
		{
			name:    "unknown user",
			userID:  user.ID + 1_000_000,
			items:   []store.Item{{SKU: "BOOK-1", Quantity: 1, UnitPriceCents: 100}},
			wantErr: store.ErrUserNotFound,
		},
		{
			name:   "check violation rolls back the order",
			userID: user.ID,
			items: []store.Item{
				{SKU: "BOOK-1", Quantity: 1, UnitPriceCents: 100},
				{SKU: "BAD-1", Quantity: 0, UnitPriceCents: 100},
			},
			wantErr: store.ErrInvalidOrder,
		},
		{
			name:    "total overflow rolls back the order",
			userID:  user.ID,
			items:   []store.Item{{SKU: "GOLD-1", Quantity: 2, UnitPriceCents: 1 << 62}},
			wantErr: store.ErrInvalidOrder,
		},
		{
			name:    "no items",
			userID:  user.ID,
			wantErr: store.ErrInvalidOrder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := s.CreateOrder(t.Context(), tt.userID, tt.items)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.userID, order.UserID)
			require.Equal(t, tt.wantTotal, order.TotalCents)
			require.Equal(t, tt.items, order.Items)
		})
	}

	var orders, items int
	err = pool.QueryRow(t.Context(),
		`SELECT count(DISTINCT o.id), count(i.id)
		 FROM orders o JOIN order_items i ON i.order_id = o.id
		 WHERE o.user_id = $1`, user.ID,
	).Scan(&orders, &items)
	require.NoError(t, err)
	require.Equal(t, 1, orders, "failed orders must leave no rows behind")
	require.Equal(t, 2, items)
}

func testListOrders(t *testing.T, s *store.Store) {
	ctx := t.Context()

	user, err := s.CreateUser(ctx, "pages@example.com", "Pager")
	require.NoError(t, err)

	const total = 5
	created := make([]int64, 0, total)
	for i := range total {
		order, err := s.CreateOrder(ctx, user.ID, []store.Item{{SKU: fmt.Sprintf("SKU-%d", i), Quantity: 1, UnitPriceCents: 100}})
		require.NoError(t, err)
		created = append(created, order.ID)
	}

	var (
		seen  []int64
		pages int
		after *store.Cursor
	)
	for {
		page, err := s.ListOrders(ctx, user.ID, after, 2)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Orders), 2)
		for _, o := range page.Orders {
			seen = append(seen, o.ID)
		}
		pages++
		if page.Next == nil {
			break
		}
		after = page.Next
	}

	require.Equal(t, 3, pages)
	slices.Reverse(created)
	require.Equal(t, created, seen)

	_, err = s.ListOrders(ctx, user.ID+1_000_000, nil, 2)
	require.ErrorIs(t, err, store.ErrUserNotFound)

	empty, err := s.CreateUser(ctx, "empty@example.com", "Nobody")
	require.NoError(t, err)
	page, err := s.ListOrders(ctx, empty.ID, nil, 0)
	require.NoError(t, err)
	require.Empty(t, page.Orders)
	require.Nil(t, page.Next)
}
