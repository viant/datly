package locator

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"net/url"
)

type Query struct {
	query url.Values
}

func (q *Query) Names() []string {
	var result = make([]string, 0)
	for k := range q.query {
		result = append(result, k)
	}
	return result
}

func (q *Query) Value(name string) (interface{}, bool, error) {
	value, ok := q.query[name]
	if !ok {
		return nil, false, nil
	}
	if len(value) > 0 {
		return value[0], true, nil
	}
	return "", true, nil
}

// NewQuery returns query locator
func NewQuery(opts ...Option) (state.Locator, error) {
	options := NewOptions(opts)
	if options.Request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	return &Query{query: options.Request.URL.Query()}, nil
}
