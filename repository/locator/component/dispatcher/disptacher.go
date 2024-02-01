package dispatcher

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"net/http"
)

type Dispatcher struct {
	registry *repository.Registry
	service  *operator.Service
}

func (d *Dispatcher) Dispatch(ctx context.Context, path *contract.Path, request *http.Request, form *state.Form) (interface{}, error) {
	//TODO maybe extract and pass session cache value
	aComponent, err := d.registry.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	unmarshal := aComponent.UnmarshalFunc(request)
	options := aComponent.LocatorOptions(request, unmarshal)
	if form != nil {
		options = append(options, locator.WithForm(form))
	}
	options = append(aComponent.LocatorOptions(request, unmarshal), options...)
	aSession := session.New(aComponent.View, session.WithLocatorOptions(options...))
	ctx = aSession.Context(ctx, true)
	value, err := d.service.Operate(ctx, aSession, aComponent)
	return value, err
}

// New creates a dispatcher
func New(registry *repository.Registry) contract.Dispatcher {
	return &Dispatcher{
		registry: registry,
		service:  operator.New(),
	}
}
