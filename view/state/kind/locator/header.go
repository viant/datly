package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"net/http"
)

type Header struct {
	request *http.Request
	header  http.Header
}

func (q *Header) Names() []string {
	var result = make([]string, 0)
	for k := range q.header {
		result = append(result, k)
	}
	return result
}

func (q *Header) Value(ctx context.Context, name string) (interface{}, bool, error) {
	value, ok := q.header[name]
	if !ok {
		return nil, false, nil
	}
	if len(value) > 0 {
		return value[0], true, nil
	}
	return "", true, nil
}

// NewHeader returns header locator
func NewHeader(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("requestState was empty")
	}
	ret := &Header{request: options.request, header: options.Header}
	if len(ret.header) == 0 {
		ret.header = ret.request.Header
	}
	return ret, nil
}
