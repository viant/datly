package http

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

type failingJSONPayload struct{}

func (failingJSONPayload) MarshalJSON() ([]byte, error) {
	return nil, errors.New("private database credentials")
}

func TestWriteJSON(t *testing.T) {
	const failure = `{"status":"error","message":"internal server error","error":"internal server error"}`
	for _, tc := range []struct {
		name               string
		payload            any
		status, wantStatus int
		wantBody           string
	}{
		{"valid", struct {
			Value int `json:"value"`
		}{42}, http.StatusCreated, http.StatusCreated, `{"value":42}`},
		{"nil", nil, http.StatusOK, http.StatusOK, ""},
		{"typed_nil", (*int)(nil), http.StatusOK, http.StatusOK, "null"},
		{"nan", math.NaN(), http.StatusOK, http.StatusInternalServerError, failure},
		{"infinity", math.Inf(1), http.StatusCreated, http.StatusInternalServerError, failure},
		{"unsupported", make(chan int), http.StatusOK, http.StatusInternalServerError, failure},
		{"marshaler_error", failingJSONPayload{}, http.StatusOK, http.StatusInternalServerError, failure},
		{"partial_object", struct {
			Value   int
			Invalid any
		}{42, make(chan int)}, http.StatusOK, http.StatusInternalServerError, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := httptest.NewRecorder()
			if tc.wantStatus == http.StatusInternalServerError {
				writer.Header().Set("Content-Length", "999")
				writer.Header().Set("Content-Encoding", "gzip")
			}
			writeJSON(writer, tc.status, tc.payload)
			if writer.Code != tc.wantStatus || writer.Body.String() != tc.wantBody {
				t.Fatalf("response = %d %q, want %d %q", writer.Code, writer.Body.String(), tc.wantStatus, tc.wantBody)
			}
			if writer.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("content type = %q", writer.Header().Get("Content-Type"))
			}
			if tc.wantStatus == http.StatusInternalServerError && (writer.Header().Get("Content-Length") != "" || writer.Header().Get("Content-Encoding") != "") {
				t.Fatalf("stale body headers: %v", writer.Header())
			}
		})
	}
}
