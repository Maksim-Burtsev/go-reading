package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/notes"
)

func newTestHandler() (http.Handler, *notes.Store) {
	store := notes.NewStore()
	return NewHandler(slog.New(slog.DiscardHandler), store, time.Second), store
}

func serve(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var v T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))
	return v
}

func TestCreateNote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantStatus  int
		wantCode    string
		wantMessage string
		wantFields  []fieldError
	}{
		{
			name:       "valid",
			body:       `{"title":"groceries","body":"milk, eggs","tags":["home","todo"]}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "valid without optional fields",
			body:       `{"title":"groceries"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "empty body",
			body:       ``,
			wantStatus: http.StatusBadRequest,
			wantCode:   "empty_body",
		},
		{
			name:       "malformed json",
			body:       `{"title":`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_json",
		},
		{
			name:        "wrong field type",
			body:        `{"title":42}`,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_json",
			wantMessage: `field "title" cannot hold a JSON number`,
		},
		{
			name:        "array instead of object",
			body:        `[]`,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_json",
			wantMessage: "request body must be a JSON object",
		},
		{
			name:       "unknown field",
			body:       `{"title":"groceries","color":"red"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "unknown_field",
		},
		{
			name:       "trailing data",
			body:       `{"title":"groceries"} {}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_json",
		},
		{
			name:       "body too large",
			body:       fmt.Sprintf(`{"title":"groceries","body":%q}`, strings.Repeat("a", maxBodyBytes)),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "body_too_large",
		},
		{
			name:       "missing title",
			body:       `{"body":"milk"}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
			wantFields: []fieldError{{Field: "title", Rule: "required"}},
		},
		{
			name:       "title too long",
			body:       fmt.Sprintf(`{"title":%q}`, strings.Repeat("ж", 201)),
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
			wantFields: []fieldError{{Field: "title", Rule: "max", Param: "200"}},
		},
		{
			name:       "duplicate tags",
			body:       `{"title":"groceries","tags":["home","home"]}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
			wantFields: []fieldError{{Field: "tags", Rule: "unique"}},
		},
		{
			name:       "too many tags",
			body:       `{"title":"groceries","tags":["a","b","c","d","e","f","g","h","i","j","k"]}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
			wantFields: []fieldError{{Field: "tags", Rule: "max", Param: "10"}},
		},
		{
			name:       "empty tag and missing title",
			body:       `{"tags":["home",""]}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
			wantFields: []fieldError{
				{Field: "title", Rule: "required"},
				{Field: "tags[1]", Rule: "required"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, store := newTestHandler()
			rec := serve(t, h, http.MethodPost, "/notes", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			if tt.wantCode != "" {
				resp := decodeBody[errorResponse](t, rec)
				require.Equal(t, tt.wantCode, resp.Error.Code)
				if tt.wantMessage != "" {
					require.Equal(t, tt.wantMessage, resp.Error.Message)
				}
				require.Equal(t, tt.wantFields, resp.Error.Fields)
				require.Empty(t, store.List())
				return
			}

			resp := decodeBody[noteResponse](t, rec)
			require.Equal(t, "/notes/"+resp.ID, rec.Header().Get("Location"))
			require.NotNil(t, resp.Tags)

			stored, err := store.Get(resp.ID)
			require.NoError(t, err)
			require.Equal(t, newNoteResponse(stored), resp)
		})
	}
}

func TestNoteRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		wantStatus int
		wantCode   string
		wantAllow  string
		check      func(t *testing.T, rec *httptest.ResponseRecorder, store *notes.Store, seeded notes.Note)
	}{
		{
			name:       "get existing",
			method:     http.MethodGet,
			target:     "/notes/{id}",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *notes.Store, seeded notes.Note) {
				require.Equal(t, newNoteResponse(seeded), decodeBody[noteResponse](t, rec))
			},
		},
		{
			name:       "get missing",
			method:     http.MethodGet,
			target:     "/notes/missing",
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "update existing",
			method:     http.MethodPut,
			target:     "/notes/{id}",
			body:       `{"title":"renamed","tags":["work"]}`,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, store *notes.Store, seeded notes.Note) {
				resp := decodeBody[noteResponse](t, rec)
				require.Equal(t, seeded.ID, resp.ID)
				require.Equal(t, "renamed", resp.Title)
				require.Empty(t, resp.Body)
				require.Equal(t, []string{"work"}, resp.Tags)

				stored, err := store.Get(seeded.ID)
				require.NoError(t, err)
				require.Equal(t, newNoteResponse(stored), resp)
			},
		},
		{
			name:       "update missing",
			method:     http.MethodPut,
			target:     "/notes/missing",
			body:       `{"title":"renamed"}`,
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "update invalid",
			method:     http.MethodPut,
			target:     "/notes/{id}",
			body:       `{"title":""}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "validation_failed",
		},
		{
			name:       "delete existing",
			method:     http.MethodDelete,
			target:     "/notes/{id}",
			wantStatus: http.StatusNoContent,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, store *notes.Store, seeded notes.Note) {
				require.Empty(t, rec.Body.String())
				_, err := store.Get(seeded.ID)
				require.ErrorIs(t, err, notes.ErrNotFound)
			},
		},
		{
			name:       "delete missing",
			method:     http.MethodDelete,
			target:     "/notes/missing",
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "method not allowed on item",
			method:     http.MethodPatch,
			target:     "/notes/{id}",
			wantStatus: http.StatusMethodNotAllowed,
			wantCode:   "method_not_allowed",
			wantAllow:  "GET, PUT, DELETE",
		},
		{
			name:       "method not allowed on collection",
			method:     http.MethodDelete,
			target:     "/notes",
			wantStatus: http.StatusMethodNotAllowed,
			wantCode:   "method_not_allowed",
			wantAllow:  "GET, POST",
		},
		{
			name:       "unknown route",
			method:     http.MethodGet,
			target:     "/unknown",
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "health check",
			method:     http.MethodGet,
			target:     "/healthz",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *notes.Store, _ notes.Note) {
				require.Equal(t, map[string]string{"status": "ok"}, decodeBody[map[string]string](t, rec))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, store := newTestHandler()
			seeded := store.Create(notes.Input{Title: "groceries", Body: "milk", Tags: []string{"home"}})

			target := strings.ReplaceAll(tt.target, "{id}", seeded.ID)
			rec := serve(t, h, tt.method, target, tt.body)

			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			require.Equal(t, tt.wantAllow, rec.Header().Get("Allow"))
			if tt.wantCode != "" {
				require.Equal(t, tt.wantCode, decodeBody[errorResponse](t, rec).Error.Code)
			}
			if tt.check != nil {
				tt.check(t, rec, store, seeded)
			}
		})
	}
}

func TestListNotes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		titles []string
	}{
		{name: "empty", titles: nil},
		{name: "several", titles: []string{"first", "second", "third"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, store := newTestHandler()
			for _, title := range tt.titles {
				store.Create(notes.Input{Title: title})
			}

			rec := serve(t, h, http.MethodGet, "/notes", "")

			require.Equal(t, http.StatusOK, rec.Code)
			resp := decodeBody[listNotesResponse](t, rec)
			require.NotNil(t, resp.Notes)
			got := make([]string, 0, len(resp.Notes))
			for _, n := range resp.Notes {
				got = append(got, n.Title)
			}
			require.ElementsMatch(t, tt.titles, got)
		})
	}
}
