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
	getRequest func() (*http.Request, error)
}

func (p *HttpRequest) Names() []string {
	return nil
}

func (p *HttpRequest) Value(ctx context.Context, name string) (interface{}, bool, error) {
	value, err := p.getRequest()
	return value, true, err
}

// NewHttpRequest returns http request locator
func NewHttpRequest(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	return &HttpRequest{
		getRequest: options.GetRequest,
	}, nil
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
