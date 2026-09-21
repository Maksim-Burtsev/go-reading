// Package httpapi exposes notes over a JSON HTTP API.
package httpapi

import (
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/notes"
)

type noteStore interface {
	Create(in notes.Input) notes.Note
	Get(id string) (notes.Note, error)
	List() []notes.Note
	Update(id string, in notes.Input) (notes.Note, error)
	Delete(id string) error
}

type api struct {
	logger   *slog.Logger
	store    noteStore
	validate *validator.Validate
}

type handlerFunc func(w http.ResponseWriter, r *http.Request) error

// NewHandler returns the root handler of the notes API with the middleware
// chain applied. A request whose handler runs longer than timeout is answered
// with 503 and its context is cancelled.
func NewHandler(logger *slog.Logger, store noteStore, timeout time.Duration) http.Handler {
	a := &api{
		logger:   logger,
		store:    store,
		validate: newValidator(),
	}

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", a.handle(a.healthz))
	mux.Handle("POST /notes", a.handle(a.createNote))
	mux.Handle("GET /notes", a.handle(a.listNotes))
	mux.Handle("GET /notes/{id}", a.handle(a.getNote))
	mux.Handle("PUT /notes/{id}", a.handle(a.updateNote))
	mux.Handle("DELETE /notes/{id}", a.handle(a.deleteNote))
	mux.Handle("/notes", a.methodNotAllowed(http.MethodGet, http.MethodPost))
	mux.Handle("/notes/{id}", a.methodNotAllowed(http.MethodGet, http.MethodPut, http.MethodDelete))
	mux.Handle("/", a.handle(func(http.ResponseWriter, *http.Request) error { return errRouteNotFound }))

	var h http.Handler = mux
	h = a.recoverPanics(h)
	h = withTimeout(h, timeout)
	h = a.logRequests(h)
	h = withRequestID(h)
	return h
}

func (a *api) handle(fn handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			a.writeError(w, r, err)
		}
	})
}

func (a *api) methodNotAllowed(methods ...string) http.Handler {
	allow := strings.Join(methods, ", ")
	return a.handle(func(w http.ResponseWriter, _ *http.Request) error {
		w.Header().Set("Allow", allow)
		return errMethodNotAllowed
	})
}

func (a *api) healthz(w http.ResponseWriter, r *http.Request) error {
	a.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

func newValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(f reflect.StructField) string {
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			return ""
		}
		return name
	})
	return v
}
