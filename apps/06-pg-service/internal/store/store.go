// Package store persists users and orders in Postgres. It owns transaction
// boundaries and translates database failures into the errors declared here.
package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/db/sqlc"
)

// Page sizes accepted by ListOrders.
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

const usersEmailKey = "users_email_key"

var (
	// ErrUserNotFound is returned when the referenced user does not exist.
	ErrUserNotFound = errors.New("user not found")
	// ErrEmailTaken is returned when another user already has the email.
	ErrEmailTaken = errors.New("email already taken")
	// ErrInvalidOrder is returned when an order has no items or violates a
	// database constraint.
	ErrInvalidOrder = errors.New("invalid order")
)

// User is a registered customer.
type User struct {
	ID        int64
	Email     string
	Name      string
	CreatedAt time.Time
}

// Item is a single order line.
type Item struct {
	SKU            string
	Quantity       int32
	UnitPriceCents int64
}

// Order is a placed order. Items is populated only by CreateOrder.
type Order struct {
	ID         int64
	UserID     int64
	TotalCents int64
	CreatedAt  time.Time
	Items      []Item
}

// OrderPage is one page of a user's orders. Next is nil on the last page.
type OrderPage struct {
	Orders []Order
	Next   *Cursor
}

// Store is a Postgres-backed repository of users and orders.
type Store struct {
	pool    *pgxpool.Pool
	queries *sqlc.Queries
}

// New returns a Store that runs queries on pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, queries: sqlc.New(pool)}
}

// CreateUser inserts a user and returns ErrEmailTaken if the email is in use.
func (s *Store) CreateUser(ctx context.Context, email, name string) (User, error) {
	row, err := s.queries.CreateUser(ctx, sqlc.CreateUserParams{Email: email, Name: name})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == usersEmailKey {
			return User{}, ErrEmailTaken
		}
		return User{}, fmt.Errorf("insert user: %w", err)
	}
	return User(row), nil
}

// GetUser returns the user with the given ID or ErrUserNotFound.
func (s *Store) GetUser(ctx context.Context, id int64) (User, error) {
	row, err := s.queries.GetUser(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("select user %d: %w", id, err)
	}
	return User(row), nil
}

// CreateOrder atomically places an order with its items for an existing user
// and returns it with the total computed by the database.
func (s *Store) CreateOrder(ctx context.Context, userID int64, items []Item) (Order, error) {
	if len(items) == 0 {
		return Order{}, fmt.Errorf("%w: no items", ErrInvalidOrder)
	}

	var order Order
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)

		if _, err := q.LockUser(ctx, userID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUserNotFound
			}
			return fmt.Errorf("lock user %d: %w", userID, err)
		}

		row, err := q.CreateOrder(ctx, userID)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}

		params := make([]sqlc.InsertOrderItemsParams, 0, len(items))
		for _, it := range items {
			params = append(params, sqlc.InsertOrderItemsParams{
				OrderID:        row.ID,
				Sku:            it.SKU,
				Quantity:       it.Quantity,
				UnitPriceCents: it.UnitPriceCents,
			})
		}
		if _, err := q.InsertOrderItems(ctx, params); err != nil {
			return fmt.Errorf("copy order items: %w", err)
		}

		row, err = q.UpdateOrderTotal(ctx, row.ID)
		if err != nil {
			return fmt.Errorf("update order total: %w", err)
		}

		order = orderFromRow(row)
		order.Items = slices.Clone(items)
		return nil
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == pgerrcode.CheckViolation || pgErr.Code == pgerrcode.NumericValueOutOfRange) {
			return Order{}, fmt.Errorf("%w: %w", ErrInvalidOrder, err)
		}
		return Order{}, fmt.Errorf("create order for user %d: %w", userID, err)
	}
	return order, nil
}

// ListOrders returns a page of the user's orders, newest first, starting
// strictly after the cursor. A nil cursor starts from the newest order. The
// limit is clamped to MaxPageSize, and a non-positive limit means
// DefaultPageSize.
func (s *Store) ListOrders(ctx context.Context, userID int64, after *Cursor, limit int) (OrderPage, error) {
	if _, err := s.GetUser(ctx, userID); err != nil {
		return OrderPage{}, err
	}

	if limit <= 0 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	rowLimit := int64(limit) + 1

	var (
		rows []sqlc.Order
		err  error
	)
	if after == nil {
		rows, err = s.queries.ListOrders(ctx, sqlc.ListOrdersParams{UserID: userID, RowLimit: rowLimit})
	} else {
		rows, err = s.queries.ListOrdersAfter(ctx, sqlc.ListOrdersAfterParams{
			UserID:         userID,
			AfterCreatedAt: after.CreatedAt,
			AfterID:        after.ID,
			RowLimit:       rowLimit,
		})
	}
	if err != nil {
		return OrderPage{}, fmt.Errorf("select orders for user %d: %w", userID, err)
	}

	var page OrderPage
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		page.Next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	page.Orders = make([]Order, 0, len(rows))
	for _, row := range rows {
		page.Orders = append(page.Orders, orderFromRow(row))
	}
	return page, nil
}

func orderFromRow(row sqlc.Order) Order {
	return Order{
		ID:         row.ID,
		UserID:     row.UserID,
		TotalCents: row.TotalCents,
		CreatedAt:  row.CreatedAt,
	}
}
