package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"

	"github.com/Maksim-Burtsev/go-reading/apps/02-http-notes/internal/notes"
)

const maxBodyBytes = 64 << 10

const timeoutBody = `{"error":{"code":"timeout","message":"request timed out"}}`

type httpError struct {
	status  int
	code    string
	message string
}

func (e *httpError) Error() string {
	return e.message
}

func (e *httpError) response() errorResponse {
	return errorResponse{Error: errorDetail{Code: e.code, Message: e.message}}
}

func badRequest(code, message string) *httpError {
	return &httpError{status: http.StatusBadRequest, code: code, message: message}
}

var (
	errRouteNotFound    = &httpError{status: http.StatusNotFound, code: "not_found", message: "route not found"}
	errNoteNotFound     = &httpError{status: http.StatusNotFound, code: "not_found", message: "note not found"}
	errMethodNotAllowed = &httpError{status: http.StatusMethodNotAllowed, code: "method_not_allowed", message: "method not allowed"}
	errInternal         = &httpError{status: http.StatusInternalServerError, code: "internal", message: "internal server error"}
)

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []fieldError `json:"fields,omitempty"`
}

type fieldError struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Param string `json:"param,omitempty"`
}

func (a *api) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		a.logger.ErrorContext(r.Context(), "write response", slog.Any("error", err))
	}
}

func (a *api) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		httpErr        *httpError
		validationErrs validator.ValidationErrors
	)
	switch {
	case errors.As(err, &httpErr):
		a.writeJSON(w, r, httpErr.status, httpErr.response())
	case errors.Is(err, notes.ErrNotFound):
		a.writeJSON(w, r, errNoteNotFound.status, errNoteNotFound.response())
	case errors.As(err, &validationErrs):
		a.writeJSON(w, r, http.StatusUnprocessableEntity, errorResponse{Error: errorDetail{
			Code:    "validation_failed",
			Message: "request body failed validation",
			Fields:  newFieldErrors(validationErrs),
		}})
	default:
		a.logger.ErrorContext(r.Context(), "handle request", slog.Any("error", err))
		a.writeJSON(w, r, errInternal.status, errInternal.response())
	}
}

func newFieldErrors(errs validator.ValidationErrors) []fieldError {
	out := make([]fieldError, 0, len(errs))
	for _, fe := range errs {
		out = append(out, fieldError{Field: fe.Field(), Rule: fe.Tag(), Param: fe.Param()})
	}
	return out
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return decodeError(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return badRequest("invalid_json", "request body must contain a single JSON object")
	}
	return nil
}

func decodeError(err error) error {
	var (
		syntaxErr  *json.SyntaxError
		typeErr    *json.UnmarshalTypeError
		maxSizeErr *http.MaxBytesError
	)
	switch {
	case errors.Is(err, io.EOF):
		return badRequest("empty_body", "request body must not be empty")
	case errors.As(err, &maxSizeErr):
		return &httpError{
			status:  http.StatusRequestEntityTooLarge,
			code:    "body_too_large",
			message: fmt.Sprintf("request body must not exceed %d bytes", maxSizeErr.Limit),
		}
	case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
		return badRequest("invalid_json", "request body contains malformed JSON")
	case errors.As(err, &typeErr):
		if typeErr.Field == "" {
			return badRequest("invalid_json", "request body must be a JSON object")
		}
		return badRequest("invalid_json", fmt.Sprintf("field %q cannot hold a JSON %s", typeErr.Field, typeErr.Value))
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return badRequest("unknown_field", "request body contains "+strings.TrimPrefix(err.Error(), "json: "))
	default:
		return badRequest("invalid_json", "request body could not be decoded")
	}
}
