package testharness

import (
	"bytes"
	"io"
	"net/http"
	"net/url"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
)

// Request is a synthetic HTTP provider scope for tests.
type Request struct {
	HTTP       *http.Request
	PathParams map[string]string
}

func NewRequest(method, path string) Request {
	request, _ := http.NewRequest(method, path, nil)
	return Request{HTTP: request}
}

func (r Request) WithRequest(request *http.Request) Request {
	r.HTTP = request
	return r
}

func (r Request) WithPathParams(pathParams map[string]string) Request {
	r.PathParams = pathParams
	return r
}

func (r Request) WithQuery(query url.Values) Request {
	request := r.ensureRequest()
	request.URL.RawQuery = query.Encode()
	r.HTTP = request
	return r
}

func (r Request) WithHeaders(headers http.Header) Request {
	request := r.ensureRequest()
	request.Header = headers.Clone()
	r.HTTP = request
	return r
}

func (r Request) WithBody(body []byte, contentType string) Request {
	request := r.ensureRequest()
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	request.Header.Set("Content-Type", contentType)
	r.HTTP = request
	return r
}

func (r Request) Providers() []locator.Provider {
	scope, _ := r.Scope()
	return scope.Providers()
}

func (r Request) Scope() (*requestprovider.Scope, error) {
	request := r.HTTP
	if request == nil {
		request, _ = http.NewRequest(http.MethodGet, "/", nil)
	}
	return requestprovider.New(request, requestprovider.WithPathParams(r.PathParams))
}

func (r Request) ensureRequest() *http.Request {
	if r.HTTP != nil {
		return r.HTTP.Clone(r.HTTP.Context())
	}
	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	return request
}
