// Package api serves the delivery HTTP API.
package api

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/dispatch"
	"github.com/Maksim-Burtsev/go-reading/apps/04-worker-pool/internal/status"
)

const retryAfterSeconds = "1"

var (
	errInvalidURL     = errors.New("url must be an absolute http or https URL")
	errMissingPayload = errors.New("payload is required")
)

type queue interface {
	Push(t dispatch.Task) error
}

type store interface {
	Track(t dispatch.Task) status.Record
	Forget(id string)
	Get(id string) (status.Record, bool)
}

type handler struct {
	queue   queue
	store   store
	logger  *slog.Logger
	maxBody int64
}

// NewHandler returns the delivery API. POST /deliveries enqueues a delivery
// and GET /deliveries/{id} reports its state. Request bodies larger than
// maxBody bytes are rejected.
func NewHandler(q queue, s store, logger *slog.Logger, maxBody int64) http.Handler {
	h := &handler{queue: q, store: s, logger: logger, maxBody: maxBody}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /deliveries", h.create)
	mux.HandleFunc("GET /deliveries/{id}", h.get)
	return mux
}

type createRequest struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
}

func (req createRequest) validate() error {
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errInvalidURL
	}
	if len(req.Payload) == 0 || bytes.Equal(req.Payload, []byte("null")) {
		return errMissingPayload
	}
	return nil
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.fail(w, r, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		h.fail(w, r, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.validate(); err != nil {
		h.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}

	task := dispatch.Task{ID: rand.Text(), URL: req.URL, Payload: req.Payload}
	rec := h.store.Track(task)
	if err := h.queue.Push(task); err != nil {
		h.store.Forget(task.ID)
		h.rejectPush(w, r, err)
		return
	}

	w.Header().Set("Location", "/deliveries/"+task.ID)
	h.respond(w, r, http.StatusAccepted, rec)
}

func (h *handler) rejectPush(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, dispatch.ErrQueueFull):
		w.Header().Set("Retry-After", retryAfterSeconds)
		h.fail(w, r, http.StatusServiceUnavailable, "delivery queue is full")
	case errors.Is(err, dispatch.ErrQueueClosed):
		h.fail(w, r, http.StatusServiceUnavailable, "server is shutting down")
	default:
		h.logger.ErrorContext(r.Context(), "enqueue delivery", "error", err)
		h.fail(w, r, http.StatusInternalServerError, "internal error")
	}
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	rec, ok := h.store.Get(r.PathValue("id"))
	if !ok {
		h.fail(w, r, http.StatusNotFound, "delivery not found")
		return
	}
	h.respond(w, r, http.StatusOK, rec)
}

func (h *handler) respond(w http.ResponseWriter, r *http.Request, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.ErrorContext(r.Context(), "write response", "error", err)
	}
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, code int, msg string) {
	h.respond(w, r, code, map[string]string{"error": msg})
}
