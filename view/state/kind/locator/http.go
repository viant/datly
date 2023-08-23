package locator

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"io"
	"net/http"
)

type HttpRequest struct {
	Request *http.Request
}

func (p *HttpRequest) Names() []string {
	return nil
}

func (p *HttpRequest) Value(ctx context.Context, name string) (interface{}, bool, error) {
	return CloneHTTPRequest(p.Request), true, nil
}

// NewHttpRequest returns http request locator
func NewHttpRequest(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.Request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	ret, err := CloneHTTPRequest(options.Request)
	if err != nil {
		return nil, err
	}
	return &HttpRequest{
		Request: ret,
	}, nil
}

// CloneHTTPRequest clones http request
func CloneHTTPRequest(request *http.Request) (*http.Request, error) {
	var data []byte
	var err error
	ret := *request
	if request.Body != nil {
		if data, err = readRequestBody(request); err != nil {
			return nil, err
		}
		ret.Body = io.NopCloser(bytes.NewReader(data))
	}
	return &ret, err
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
