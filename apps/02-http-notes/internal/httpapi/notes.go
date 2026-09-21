package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/notes"
)

type noteRequest struct {
	Title string   `json:"title" validate:"required,max=200"`
	Body  string   `json:"body" validate:"max=10000"`
	Tags  []string `json:"tags" validate:"max=10,unique,dive,required,max=32"`
}

func (req noteRequest) input() notes.Input {
	return notes.Input{
		Title: req.Title,
		Body:  req.Body,
		Tags:  req.Tags,
	}
}

type noteResponse struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Tags      []string  `json:"tags"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func newNoteResponse(n notes.Note) noteResponse {
	tags := n.Tags
	if tags == nil {
		tags = []string{}
	}
	return noteResponse{
		ID:        n.ID,
		Title:     n.Title,
		Body:      n.Body,
		Tags:      tags,
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

type listNotesResponse struct {
	Notes []noteResponse `json:"notes"`
}

func (a *api) createNote(w http.ResponseWriter, r *http.Request) error {
	req, err := a.decodeNoteRequest(w, r)
	if err != nil {
		return err
	}

	n := a.store.Create(req.input())

	w.Header().Set("Location", "/notes/"+n.ID)
	a.writeJSON(w, r, http.StatusCreated, newNoteResponse(n))
	return nil
}

func (a *api) listNotes(w http.ResponseWriter, r *http.Request) error {
	all := a.store.List()

	resp := listNotesResponse{Notes: make([]noteResponse, 0, len(all))}
	for _, n := range all {
		resp.Notes = append(resp.Notes, newNoteResponse(n))
	}
	a.writeJSON(w, r, http.StatusOK, resp)
	return nil
}

func (a *api) getNote(w http.ResponseWriter, r *http.Request) error {
	n, err := a.store.Get(r.PathValue("id"))
	if err != nil {
		return err
	}
	a.writeJSON(w, r, http.StatusOK, newNoteResponse(n))
	return nil
}

func (a *api) updateNote(w http.ResponseWriter, r *http.Request) error {
	req, err := a.decodeNoteRequest(w, r)
	if err != nil {
		return err
	}

	n, err := a.store.Update(r.PathValue("id"), req.input())
	if err != nil {
		return err
	}
	a.writeJSON(w, r, http.StatusOK, newNoteResponse(n))
	return nil
}

func (a *api) deleteNote(w http.ResponseWriter, r *http.Request) error {
	if err := a.store.Delete(r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *api) decodeNoteRequest(w http.ResponseWriter, r *http.Request) (noteRequest, error) {
	var req noteRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return noteRequest{}, err
	}
	if err := a.validate.Struct(req); err != nil {
		return noteRequest{}, fmt.Errorf("validate note request: %w", err)
	}
	return req, nil
}
