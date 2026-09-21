// Package server exposes the health and metrics endpoints.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	healthTimeout = 2 * time.Second
	statusOK      = "ok"
	statusDown    = "unavailable"
)

// Pinger checks that a dependency is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

type healthResponse struct {
	Status       string            `json:"status"`
	Dependencies map[string]string `json:"dependencies"`
}

// NewHandler serves GET /metrics from reg and GET /health, which pings every
// dependency concurrently and answers 503 if any of them fails.
func NewHandler(logger *slog.Logger, reg *prometheus.Registry, deps map[string]Pinger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", handleHealth(logger, deps))
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return mux
}

func handleHealth(logger *slog.Logger, deps map[string]Pinger) http.Handler {
	type result struct {
		name string
		err  error
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
		defer cancel()

		results := make(chan result, len(deps))
		for name, dep := range deps {
			go func() { results <- result{name: name, err: dep.Ping(ctx)} }()
		}

		code := http.StatusOK
		resp := healthResponse{Status: statusOK, Dependencies: make(map[string]string, len(deps))}
		for range len(deps) {
			res := <-results
			if res.err != nil {
				logger.ErrorContext(ctx, "health check failed", "dependency", res.name, "error", res.err)
				code, resp.Status = http.StatusServiceUnavailable, statusDown
				resp.Dependencies[res.name] = res.err.Error()
				continue
			}
			resp.Dependencies[res.name] = statusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	})
}
