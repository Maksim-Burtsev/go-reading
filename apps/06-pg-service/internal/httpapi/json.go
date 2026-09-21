package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Maksim-Burtsev/go-reading/apps/06-pg-service/internal/store"
)

const maxBodyBytes = 1 << 20

type createUserRequest struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

type createOrderRequest struct {
	Items []itemPayload `json:"items"`
}

type itemPayload struct {
	SKU            string `json:"sku"`
	Quantity       int32  `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type userResponse struct {
	ID        int64     `json:"id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type orderResponse struct {
	ID         int64         `json:"id"`
	UserID     int64         `json:"user_id"`
	TotalCents int64         `json:"total_cents"`
	CreatedAt  time.Time     `json:"created_at"`
	Items      []itemPayload `json:"items,omitempty"`
}

type orderPageResponse struct {
	Orders     []orderResponse `json:"orders"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func newUserResponse(u store.User) userResponse {
	return userResponse{ID: u.ID, Email: u.Email, Name: u.Name, CreatedAt: u.CreatedAt.UTC()}
}

func newOrderResponse(o store.Order) orderResponse {
	resp := orderResponse{
		ID:         o.ID,
		UserID:     o.UserID,
		TotalCents: o.TotalCents,
		CreatedAt:  o.CreatedAt.UTC(),
	}
	for _, it := range o.Items {
		resp.Items = append(resp.Items, itemPayload(it))
	}
	return resp
}

func newOrderPageResponse(p store.OrderPage) orderPageResponse {
	resp := orderPageResponse{Orders: make([]orderResponse, 0, len(p.Orders))}
	for _, o := range p.Orders {
		resp.Orders = append(resp.Orders, newOrderResponse(o))
	}
	if p.Next != nil {
		resp.NextCursor = p.Next.Encode()
	}
	return resp
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func (h *handler) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.ErrorContext(r.Context(), "write response", slog.Any("error", err))
	}
}

func (h *handler) writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	h.writeJSON(w, r, status, errorResponse{Error: msg})
}
