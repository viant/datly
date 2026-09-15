package readerbuilder

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPStatelessInspect(t *testing.T) {
	handler := New(Config{Name: "Records"})
	body := `{"dql":"#setting($_ = $route('/records','GET'))\nSELECT records.* FROM (SELECT id FROM records) records","operation":{"type":"inspect"}}`
	request := httptest.NewRequest(http.MethodPost, "/v1/datly/reader-builder", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"applied":true`) || !strings.Contains(response.Body.String(), `"component"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPRejectsUnknownInputField(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"dql":"x","unknown":true,"operation":{"type":"inspect"}}`))
	response := httptest.NewRecorder()
	New(Config{}).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
