package component

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"net/http"
)

type componentLocator struct {
	custom     []interface{}
	dispatch   contract.Dispatcher
	getRequest func() (*http.Request, error)
	getForm    func() *state.Form
}

func (l *componentLocator) Names() []string {
	return nil
}

func (l *componentLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	method, URI := shared.ExtractPath(name)
	request, err := l.getRequest()
	if err != nil {
		return nil, false, err
	}
	form := l.getForm()
	value, err := l.dispatch.Dispatch(ctx, &contract.Path{Method: method, URI: URI}, request, form)
	return value, err == nil, err
}

// TODO passed locator options to dispatcher so that this wil not be nil
var dispatcher contract.Dispatcher

// newComponentLocator returns component locator
func newComponentLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	if options.Dispatcher == nil {
		options.Dispatcher = dispatcher
	}
	if options.Dispatcher == nil {
		return nil, fmt.Errorf("dispatcher was empty")
	}
	dispatcher = options.Dispatcher
	ret := &componentLocator{
		custom:     options.Custom,
		dispatch:   options.Dispatcher,
		getRequest: options.GetRequest,
		getForm:    options.GetForm,
	}
	return ret, nil
}

func init() {
	locator.Register(state.KindComponent, newComponentLocator)
}
