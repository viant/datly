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
}

func (p *HttpRequest) Names() []string {
	return nil
}

func (p *HttpRequest) Value(ctx context.Context, name string) (interface{}, bool, error) {
	switch strings.ToLower(name) {
	case "header":
		value, err := p.getRequest()
		if err != nil {
			return nil, false, err
		}
		return value.Header, true, nil
	case "remoteaddr":
		value, err := p.getRequest()
		if err != nil {
			return nil, false, err
		}
		return value.RemoteAddr, true, nil

	case "method":
		value, err := p.getRequest()
		if err != nil {
			return nil, false, err
		}
		return value.Method, true, nil
	}
	value, err := p.getRequest()
	return value, true, err
}

// NewHttpRequest returns http requestState locator
func NewHttpRequest(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("requestState was empty")
	}
	return &HttpRequest{
		getRequest: options.GetRequest,
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
