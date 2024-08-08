package dispatcher

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state/kind/locator"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
)

type Dispatcher struct {
	registry *repository.Registry
	service  *operator.Service
}

func (d *Dispatcher) Dispatch(ctx context.Context, path *contract.Path, request *http.Request, form *hstate.Form, opts ...contract.Option) (interface{}, error) {
	//TODO maybe extract and pass session cache value
	aComponent, err := d.registry.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	unmarshal := aComponent.UnmarshalFunc(request)
	var options = aComponent.LocatorOptions(request, form, unmarshal)
	cOptions := contract.NewOptions(opts...)
	if cOptions.Constants != nil {
		options = append(options, locator.WithConstants(cOptions.Constants))
	}
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
