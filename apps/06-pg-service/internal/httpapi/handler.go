// Package httpapi exposes users and orders as a JSON HTTP API.
package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/mail"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

const (
	maxNameLength           = 200
	maxOrderItems           = 100
	maxIdempotencyKeyLength = 255
)

var (
	errInvalidID    = errors.New("id must be a positive integer")
	errInvalidEmail = errors.New("email is invalid")
)

// Store is the persistence layer the API depends on.
type Store interface {
	CreateUser(ctx context.Context, email, name string) (store.User, error)
	GetUser(ctx context.Context, id int64) (store.User, error)
	CreateOrder(ctx context.Context, userID int64, key string, items []store.Item) (store.Order, error)
	ListOrders(ctx context.Context, userID int64, after *store.Cursor, limit int) (store.OrderPage, error)
}

type handler struct {
	logger *slog.Logger
	store  Store
}

// NewHandler returns the API router wrapped in request ID, client IP, request
// logging and panic recovery middleware. The client IP is taken from
// X-Forwarded-For only when the request passed through trustedProxies.
func NewHandler(logger *slog.Logger, s Store, trustedProxies []netip.Prefix) http.Handler {
	h := &handler{logger: logger, store: s}

	r := chi.NewRouter()
	r.Use(middleware.RequestID, clientIP(trustedProxies), logRequests(logger), middleware.Recoverer)

	r.Post("/users", h.createUser)
	r.Get("/users/{id}", h.getUser)
	r.Post("/users/{id}/orders", h.createOrder)
	r.Get("/users/{id}/orders", h.listOrders)

	return r
}

func (h *handler) createUser(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	email, err := normalizeEmail(req.Email)
	if err != nil {
		h.writeError(w, r, http.StatusUnprocessableEntity, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || utf8.RuneCountInString(name) > maxNameLength {
		h.writeError(w, r, http.StatusUnprocessableEntity, fmt.Sprintf("name must be 1 to %d characters", maxNameLength))
		return
	}
	if strings.ContainsRune(name, 0) {
		h.writeError(w, r, http.StatusUnprocessableEntity, "name must not contain NUL characters")
		return
	}

	user, err := h.store.CreateUser(r.Context(), email, name)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusCreated, newUserResponse(user))
}

func (h *handler) getUser(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	user, err := h.store.GetUser(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, newUserResponse(user))
}

func (h *handler) createOrder(w http.ResponseWriter, r *http.Request) {
	userID, err := pathID(r)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKeyLength {
		h.writeError(w, r, http.StatusBadRequest, fmt.Sprintf("Idempotency-Key must be at most %d bytes", maxIdempotencyKeyLength))
		return
	}

	var req createOrderRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateItems(req.Items); err != nil {
		h.writeError(w, r, http.StatusUnprocessableEntity, err.Error())
		return
	}

	items := make([]store.Item, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, store.Item(it))
	}

	order, err := h.store.CreateOrder(r.Context(), userID, key, items)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusCreated, newOrderResponse(order))
}

func (h *handler) listOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := pathID(r)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	query := r.URL.Query()

	var limit int
	if v := query.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit <= 0 {
			h.writeError(w, r, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
	}

	var after *store.Cursor
	if v := query.Get("cursor"); v != "" {
		c, err := store.DecodeCursor(v)
		if err != nil {
			h.writeError(w, r, http.StatusBadRequest, "invalid cursor")
			return
		}
		after = &c
	}

	page, err := h.store.ListOrders(r.Context(), userID, after, limit)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, newOrderPageResponse(page))
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrUserNotFound):
		h.writeError(w, r, http.StatusNotFound, "user not found")
	case errors.Is(err, store.ErrEmailTaken):
		h.writeError(w, r, http.StatusConflict, "email already taken")
	case errors.Is(err, store.ErrInvalidOrder):
		h.writeError(w, r, http.StatusUnprocessableEntity, "order violates constraints")
	default:
		h.logger.ErrorContext(r.Context(), "request failed",
			slog.String("request_id", middleware.GetReqID(r.Context())),
			slog.Any("error", err),
		)
		h.writeError(w, r, http.StatusInternalServerError, "internal error")
	}
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errInvalidID
	}
	return id, nil
}

func normalizeEmail(s string) (string, error) {
	s = strings.TrimSpace(s)
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return "", errInvalidEmail
	}
	return strings.ToLower(addr.Address), nil
}

func validateItems(items []itemPayload) error {
	if len(items) == 0 || len(items) > maxOrderItems {
		return fmt.Errorf("an order must have 1 to %d items", maxOrderItems)
	}
	for i, it := range items {
		switch {
		case strings.TrimSpace(it.SKU) == "":
			return fmt.Errorf("items[%d].sku must not be empty", i)
		case strings.ContainsRune(it.SKU, 0):
			return fmt.Errorf("items[%d].sku must not contain NUL characters", i)
		case it.Quantity <= 0:
			return fmt.Errorf("items[%d].quantity must be positive", i)
		case it.UnitPriceCents < 0:
			return fmt.Errorf("items[%d].unit_price_cents must not be negative", i)
		}
	}
	return nil
}

func clientIP(trustedProxies []netip.Prefix) func(http.Handler) http.Handler {
	if len(trustedProxies) == 0 {
		return middleware.ClientIPFromRemoteAddr
	}
	prefixes := make([]string, 0, len(trustedProxies))
	for _, p := range trustedProxies {
		prefixes = append(prefixes, p.String())
	}
	fromXFF := middleware.ClientIPFromXFF(prefixes...)

	return func(next http.Handler) http.Handler {
		proxied, direct := fromXFF(next), middleware.ClientIPFromRemoteAddr(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if fromTrustedProxy(r.RemoteAddr, trustedProxies) {
				proxied.ServeHTTP(w, r)
				return
			}
			direct.ServeHTTP(w, r)
		})
	}
}

func fromTrustedProxy(remoteAddr string, trustedProxies []netip.Prefix) bool {
	peer, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return false
	}
	addr := peer.Addr().Unmap().WithZone("")
	return slices.ContainsFunc(trustedProxies, func(p netip.Prefix) bool { return p.Contains(addr) })
}

func logRequests(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			logger.InfoContext(r.Context(), "http request",
				slog.String("request_id", middleware.GetReqID(r.Context())),
				slog.String("method", r.Method),
				slog.String("route", chi.RouteContext(r.Context()).RoutePattern()),
				slog.String("path", r.URL.Path),
				slog.String("client_ip", middleware.GetClientIP(r.Context())),
				slog.Int("status", ww.Status()),
				slog.Int("bytes", ww.BytesWritten()),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}
