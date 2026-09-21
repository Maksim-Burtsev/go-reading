// Package server implements the HTTP API of the demo server: a health check and a small,
// rate limited API serving Go proverbs.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
)

// Proverb is a Go proverb.
type Proverb struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

type api struct {
	logger   *slog.Logger
	proverbs []Proverb
}

// New returns the handler of the demo server. Routes under /api/ pass through limit; the health
// check does not, so probes are never throttled.
func New(logger *slog.Logger, limit func(http.Handler) http.Handler) http.Handler {
	a := &api{
		logger: logger,
		proverbs: []Proverb{
			{ID: 1, Text: "Don't communicate by sharing memory, share memory by communicating."},
			{ID: 2, Text: "Concurrency is not parallelism."},
			{ID: 3, Text: "The bigger the interface, the weaker the abstraction."},
			{ID: 4, Text: "Make the zero value useful."},
			{ID: 5, Text: "Errors are values."},
			{ID: 6, Text: "Clear is better than clever."},
		},
	}

	limited := http.NewServeMux()
	limited.HandleFunc("GET /api/proverbs", a.listProverbs)
	limited.HandleFunc("GET /api/proverbs/{id}", a.getProverb)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.Handle("/api/", limit(limited))
	return mux
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(r.Context(), w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) listProverbs(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(r.Context(), w, http.StatusOK, a.proverbs)
}

func (a *api) getProverb(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		a.writeJSON(r.Context(), w, http.StatusBadRequest, map[string]string{"error": "id must be an integer"})
		return
	}
	i := slices.IndexFunc(a.proverbs, func(p Proverb) bool { return p.ID == id })
	if i < 0 {
		a.writeJSON(r.Context(), w, http.StatusNotFound, map[string]string{"error": "proverb not found"})
		return
	}
	a.writeJSON(r.Context(), w, http.StatusOK, a.proverbs[i])
}

func (a *api) writeJSON(ctx context.Context, w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		a.logger.ErrorContext(ctx, "write response", slog.Any("error", err))
	}
}
