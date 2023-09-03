package component

import (
	"context"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"net/http"
	"strings"
)

type componentLocator struct {
	custom   []interface{}
	dispatch func(ctx context.Context, path *component.Path, options ...interface{}) (interface{}, error)
}

func (l *componentLocator) Names() []string {
	return nil
}

func (l *componentLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	pair := strings.Split(name, ":")
	method := http.MethodGet
	URI := ""
	switch len(pair) {
	case 1:
		URI = name
	case 2:
		method = pair[0]
		URI = pair[1]
	}
	value, err := l.dispatch(ctx, &component.Path{Method: method, URI: URI}, l.custom...)
	return value, err == nil, err
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &componentLocator{
		custom:   options.Custom,
		dispatch: options.Dispatch,
	}
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
