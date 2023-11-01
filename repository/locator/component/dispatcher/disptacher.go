package dispatcher

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"net/http"
)

type Dispatcher struct {
	registry *repository.Registry
	service  *operator.Service
}

func (d *Dispatcher) Dispatch(ctx context.Context, path *contract.Path, request *http.Request) (interface{}, error) {
	//TODO maybe extract and pass session cache value
	aComponent, err := d.registry.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	unmarshal := aComponent.UnmarshalFunc(request)
	aSession := session.New(aComponent.View, session.WithLocatorOptions(aComponent.LocatorOptions(request, unmarshal)...))
	if err = aSession.Populate(ctx); err != nil {
		return d.service.HandleError(ctx, err, aComponent, aSession)
	}
	value, err := d.service.Operate(ctx, aComponent, aSession)

	return value, err
}

// New creates a dispatcher
func New(registry *repository.Registry) contract.Dispatcher {
	return &Dispatcher{
		registry: registry,
		service:  operator.New(),
	}
}
