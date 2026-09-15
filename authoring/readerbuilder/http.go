package readerbuilder

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const maxRequestBytes = 1 << 20

// ServeHTTP exposes the stateless builder contract. Decoded domain failures
// use the normal response envelope; malformed transport input uses HTTP 400.
func (s *Service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeHTTPError(writer, http.StatusMethodNotAllowed, "POST is required")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	var input Request
	if err := decoder.Decode(&input); err != nil {
		writeHTTPError(writer, http.StatusBadRequest, err.Error())
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeHTTPError(writer, http.StatusBadRequest, "request must contain one JSON object")
		return
	}
	_ = json.NewEncoder(writer).Encode(s.Apply(request.Context(), input))
}

func writeHTTPError(writer http.ResponseWriter, status int, message string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": message})
}
