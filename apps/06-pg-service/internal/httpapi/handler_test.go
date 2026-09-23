package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/httpapi"
	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

var createdAt = time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)

type fakeStore struct {
	err error

	gotUserID int64
	gotEmail  string
	gotKey    string
	gotItems  []store.Item
	gotAfter  *store.Cursor
	gotLimit  int
	page      store.OrderPage
}

func (f *fakeStore) CreateUser(_ context.Context, email, name string) (store.User, error) {
	f.gotEmail = email
	if f.err != nil {
		return store.User{}, f.err
	}
	return store.User{ID: 1, Email: email, Name: name, CreatedAt: createdAt}, nil
}

func (f *fakeStore) GetUser(_ context.Context, id int64) (store.User, error) {
	f.gotUserID = id
	if f.err != nil {
		return store.User{}, f.err
	}
	return store.User{ID: id, Email: "ann@example.com", Name: "Ann", CreatedAt: createdAt}, nil
}

func (f *fakeStore) CreateOrder(_ context.Context, userID int64, key string, items []store.Item) (store.Order, error) {
	f.gotUserID, f.gotKey, f.gotItems = userID, key, items
	if f.err != nil {
		return store.Order{}, f.err
	}
	return store.Order{ID: 10, UserID: userID, TotalCents: 2500, CreatedAt: createdAt, Items: items}, nil
}

func (f *fakeStore) ListOrders(_ context.Context, userID int64, after *store.Cursor, limit int) (store.OrderPage, error) {
	f.gotUserID, f.gotAfter, f.gotLimit = userID, after, limit
	if f.err != nil {
		return store.OrderPage{}, f.err
	}
	return f.page, nil
}

func serve(t *testing.T, s httpapi.Store, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	httpapi.NewHandler(slog.New(slog.DiscardHandler), s, nil).ServeHTTP(rec, req)
	return rec
}

func TestHandlerErrorMapping(t *testing.T) {
	t.Parallel()

	const (
		validUser  = `{"email":"ann@example.com","name":"Ann"}`
		validOrder = `{"items":[{"sku":"A-1","quantity":2,"unit_price_cents":1250}]}`
	)

	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		storeErr   error
		wantStatus int
		wantError  string
	}{
		{"create user: email taken", http.MethodPost, "/users", validUser, store.ErrEmailTaken, http.StatusConflict, "email already taken"},
		{"create user: store failure", http.MethodPost, "/users", validUser, errors.New("connection reset"), http.StatusInternalServerError, "internal error"},
		{"create user: malformed json", http.MethodPost, "/users", `{"email":`, nil, http.StatusBadRequest, ""},
		{"create user: unknown field", http.MethodPost, "/users", `{"email":"ann@example.com","name":"Ann","admin":true}`, nil, http.StatusBadRequest, ""},
		{"create user: invalid email", http.MethodPost, "/users", `{"email":"ann","name":"Ann"}`, nil, http.StatusUnprocessableEntity, "email is invalid"},
		{"create user: email with display name", http.MethodPost, "/users", `{"email":"Ann <ann@example.com>","name":"Ann"}`, nil, http.StatusUnprocessableEntity, "email is invalid"},
		{"create user: blank name", http.MethodPost, "/users", `{"email":"ann@example.com","name":"  "}`, nil, http.StatusUnprocessableEntity, ""},
		{"create user: NUL in name", http.MethodPost, "/users", `{"email":"ann@example.com","name":"A\u0000nn"}`, nil, http.StatusUnprocessableEntity, "name must not contain NUL characters"},
		{"get user: not found", http.MethodGet, "/users/7", "", store.ErrUserNotFound, http.StatusNotFound, "user not found"},
		{"get user: non-numeric id", http.MethodGet, "/users/abc", "", nil, http.StatusBadRequest, "id must be a positive integer"},
		{"get user: zero id", http.MethodGet, "/users/0", "", nil, http.StatusBadRequest, "id must be a positive integer"},
		{"create order: wrapped user not found", http.MethodPost, "/users/7/orders", validOrder, fmt.Errorf("create order: %w", store.ErrUserNotFound), http.StatusNotFound, "user not found"},
		{"create order: constraint violation", http.MethodPost, "/users/7/orders", validOrder, fmt.Errorf("%w: check violation", store.ErrInvalidOrder), http.StatusUnprocessableEntity, "order violates constraints"},
		{"create order: no items", http.MethodPost, "/users/7/orders", `{"items":[]}`, nil, http.StatusUnprocessableEntity, ""},
		{"create order: NUL in sku", http.MethodPost, "/users/7/orders", `{"items":[{"sku":"A-\u0000","quantity":1,"unit_price_cents":1}]}`, nil, http.StatusUnprocessableEntity, "items[0].sku must not contain NUL characters"},
		{"create order: zero quantity", http.MethodPost, "/users/7/orders", `{"items":[{"sku":"A-1","quantity":0,"unit_price_cents":1}]}`, nil, http.StatusUnprocessableEntity, "items[0].quantity must be positive"},
		{"create order: negative price", http.MethodPost, "/users/7/orders", `{"items":[{"sku":"A-1","quantity":1,"unit_price_cents":-1}]}`, nil, http.StatusUnprocessableEntity, "items[0].unit_price_cents must not be negative"},
		{"create order: quantity overflows int32", http.MethodPost, "/users/7/orders", `{"items":[{"sku":"A-1","quantity":3000000000,"unit_price_cents":1}]}`, nil, http.StatusBadRequest, ""},
		{"list orders: user not found", http.MethodGet, "/users/7/orders", "", store.ErrUserNotFound, http.StatusNotFound, "user not found"},
		{"list orders: invalid cursor", http.MethodGet, "/users/7/orders?cursor=bm9wZQ", "", nil, http.StatusBadRequest, "invalid cursor"},
		{"list orders: zero limit", http.MethodGet, "/users/7/orders?limit=0", "", nil, http.StatusBadRequest, "limit must be a positive integer"},
		{"list orders: non-numeric limit", http.MethodGet, "/users/7/orders?limit=ten", "", nil, http.StatusBadRequest, "limit must be a positive integer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serve(t, &fakeStore{err: tt.storeErr}, tt.method, tt.target, tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var body struct {
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.NotEmpty(t, body.Error)
			if tt.wantError != "" {
				require.Equal(t, tt.wantError, body.Error)
			}
		})
	}
}

func TestCreateUserNormalizesEmail(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{}
	rec := serve(t, fake, http.MethodPost, "/users", `{"email":"  Ann@Example.COM ","name":" Ann "}`)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "ann@example.com", fake.gotEmail)
	require.JSONEq(t, `{"id":1,"email":"ann@example.com","name":"Ann","created_at":"2026-09-21T10:00:00Z"}`, rec.Body.String())
}

func TestCreateOrder(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{}
	rec := serve(t, fake, http.MethodPost, "/users/42/orders", `{"items":[{"sku":"A-1","quantity":2,"unit_price_cents":1250}]}`)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, int64(42), fake.gotUserID)
	require.Equal(t, []store.Item{{SKU: "A-1", Quantity: 2, UnitPriceCents: 1250}}, fake.gotItems)
	require.JSONEq(t, `{
		"id": 10,
		"user_id": 42,
		"total_cents": 2500,
		"created_at": "2026-09-21T10:00:00Z",
		"items": [{"sku": "A-1", "quantity": 2, "unit_price_cents": 1250}]
	}`, rec.Body.String())
}

func TestCreateOrderIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
		wantKey    string
	}{
		{"no key", "", http.StatusCreated, ""},
		{"key is passed to the store", "2c1b7d0e-order-42", http.StatusCreated, "2c1b7d0e-order-42"},
		{"longest key", strings.Repeat("k", 255), http.StatusCreated, strings.Repeat("k", 255)},
		{"key too long", strings.Repeat("k", 256), http.StatusBadRequest, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeStore{}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/users/42/orders",
				strings.NewReader(`{"items":[{"sku":"A-1","quantity":2,"unit_price_cents":1250}]}`))
			if tt.key != "" {
				req.Header.Set("Idempotency-Key", tt.key)
			}
			rec := httptest.NewRecorder()
			httpapi.NewHandler(slog.New(slog.DiscardHandler), fake, nil).ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, tt.wantKey, fake.gotKey)
		})
	}
}

func TestListOrdersPagination(t *testing.T) {
	t.Parallel()

	next := store.Cursor{CreatedAt: createdAt, ID: 9}
	after := store.Cursor{CreatedAt: createdAt.Add(time.Hour), ID: 12}

	tests := []struct {
		name       string
		target     string
		page       store.OrderPage
		wantAfter  *store.Cursor
		wantLimit  int
		wantCursor string
	}{
		{
			name:      "first page with defaults",
			target:    "/users/42/orders",
			page:      store.OrderPage{Orders: []store.Order{}},
			wantLimit: 0,
		},
		{
			name:       "cursor and limit are passed through",
			target:     "/users/42/orders?limit=2&cursor=" + after.Encode(),
			page:       store.OrderPage{Orders: []store.Order{{ID: 11, UserID: 42, CreatedAt: createdAt}}, Next: &next},
			wantAfter:  &after,
			wantLimit:  2,
			wantCursor: next.Encode(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeStore{page: tt.page}
			rec := serve(t, fake, http.MethodGet, tt.target, "")

			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, int64(42), fake.gotUserID)
			require.Equal(t, tt.wantLimit, fake.gotLimit)
			if tt.wantAfter == nil {
				require.Nil(t, fake.gotAfter)
			} else {
				require.NotNil(t, fake.gotAfter)
				require.True(t, tt.wantAfter.CreatedAt.Equal(fake.gotAfter.CreatedAt))
				require.Equal(t, tt.wantAfter.ID, fake.gotAfter.ID)
			}

			var body struct {
				Orders     []json.RawMessage `json:"orders"`
				NextCursor string            `json:"next_cursor"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.NotNil(t, body.Orders)
			require.Len(t, body.Orders, len(tt.page.Orders))
			require.Equal(t, tt.wantCursor, body.NextCursor)
		})
	}
}
