package locator

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"io"
	"net/http"
	"strings"
)

type HttpRequest struct {
	getRequest func() (*http.Request, error)
	request    *http.Request
}

func (p *HttpRequest) Names() []string {
	return nil
}

func (p *HttpRequest) Value(ctx context.Context, name string) (interface{}, bool, error) {
	request := p.request
	if p.request == nil {
		var err error
		request, err = p.getRequest()
		if err != nil {
			return nil, false, err
		}
	}
	switch strings.ToLower(name) {
	case "uri":
		return request.RequestURI, true, nil
	case "header":

		return request.Header, true, nil
	case "remoteaddr":
		return request.RemoteAddr, true, nil
	case "method":
		return request.Method, true, nil
	}
	return request, true, nil
}

// NewHttpRequest returns http requestState locator
func NewHttpRequest(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("requestState was empty")
	}
	return &HttpRequest{
		getRequest: options.GetRequest,
		request:    options.request,
	}, nil
}

func readRequestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, nil
	}
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	_ = request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(data))
	return data, err
}
