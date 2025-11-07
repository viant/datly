package shared

import (
	"bytes"
	"io"
	"mime"
	"net/http"
	"strings"
)

// CloneHTTPRequest clones http request
func CloneHTTPRequest(request *http.Request) (*http.Request, error) {
	// Shallow clone; special-case multipart to avoid buffering entire body
	ret := *request
	ret.URL = request.URL

	if request.Body == nil {
		return &ret, nil
	}

	// Detect multipart/*; avoid reading/consuming body
	if IsMultipartRequest(request) {
		// share the same Body; caller must ensure only one reader consumes it
		ret.Body = request.Body
		return &ret, nil
	}

	// Non-multipart: safe full read, reset both original and clone bodies
	data, err := readRequestBody(request)
	if err != nil {
		return nil, err
	}
	ret.Body = io.NopCloser(bytes.NewReader(data))
	return &ret, nil
}

func readRequestBody(request *http.Request) ([]byte, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	_ = request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(data))
	return data, err
}

// IsMultipartRequest returns true if request Content-Type is multipart/*
func IsMultipartRequest(r *http.Request) bool {
	if r == nil || r.Header == nil {
		return false
	}
	return IsMultipartContentType(r.Header.Get("Content-Type"))
}

// IsMultipartContentType returns true when the Content-Type header indicates any multipart/*
func IsMultipartContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return strings.Contains(strings.ToLower(ct), "multipart/")
	}
	return strings.HasPrefix(strings.ToLower(mediaType), "multipart/")
}

// IsFormData returns true when mediaType equals multipart/form-data
func IsFormData(mediaType string) bool {
	return strings.EqualFold(mediaType, "multipart/form-data")
}
