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
		// If multipart form has already been parsed, we don't need to
		// share or re-read the body. Instead, reuse the parsed form and
		// multipart data on the clone so that downstream logic can access
		// form values without touching the body again.
		if request.MultipartForm != nil {
			// Body is no longer needed for form access.
			ret.Body = http.NoBody
			// Reuse parsed forms and multipart metadata.
			ret.MultipartForm = request.MultipartForm
			if request.Form != nil {
				ret.Form = request.Form
			}
			if request.PostForm != nil {
				ret.PostForm = request.PostForm
			}

			return &ret, nil
		}

		// Backwards compatibility: if the multipart form hasn't been
		// parsed yet, fall back to sharing the body. Callers must
		// still ensure only one reader consumes it.
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
