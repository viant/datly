package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/xdatly/handler/exec"
	"net/url"
)

type Query struct {
	query    url.Values
	rawQuery string
}

func (q *Query) Names() []string {
	var result = make([]string, 0)
	for k := range q.query {
		result = append(result, k)
	}
	return result
}

func (q *Query) Value(ctx context.Context, name string) (interface{}, bool, error) {
	if name == "" {
		return q.rawQuery, true, nil
	}
	value, ok := q.query[name]
	if !ok {
		return nil, false, nil
	}
	if len(value) == 1 && value[0] == "" && q.ignoreEmptyParameters(ctx) {
		return "", false, nil
	}
	switch len(value) {
	case 0:
	case 1:
		return value[0], true, nil
	default:
		return value, true, nil
	}
	if q.ignoreEmptyParameters(ctx) {
		return "", false, nil
	}
	return "", true, nil
}

func (q *Query) ignoreEmptyParameters(ctx context.Context) bool {
	ignoreEmptyParameters := false
	if value := ctx.Value(exec.ContextKey); value != nil {
		ignoreEmptyParameters = value.(*exec.Context).IgnoreEmptyQueryParameters
	}
	return ignoreEmptyParameters
}

// NewQuery returns query locator
func NewQuery(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("requestState was empty")
	}

	return &Query{query: options.request.URL.Query(), rawQuery: options.request.URL.RawQuery}, nil
}
