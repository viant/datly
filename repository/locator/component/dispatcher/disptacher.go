package dispatcher

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state/kind/locator"
)

type Dispatcher struct {
	registry *repository.Registry
	auth     *auth.Service
	service  *operator.Service
}

func (d *Dispatcher) Dispatch(ctx context.Context, path *contract.Path, opts ...contract.Option) (interface{}, error) {
	//TODO maybe extract and pass session cache value
	aComponent, err := d.registry.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	cOptions := contract.NewOptions(opts...)
	request := cOptions.Request
	if request == nil {
		return nil, fmt.Errorf("failed to dispatch %v %v request was empty", path.Method, path.URI)
	}

	unmarshal := aComponent.UnmarshalFunc(request)
	var options = aComponent.LocatorOptions(request, cOptions.Form, unmarshal)
	if cOptions.Constants != nil {
		options = append(options, locator.WithConstants(cOptions.Constants))
	}
	if cOptions.PathParameters != nil {
		options = append(options, locator.WithPathParameters(cOptions.PathParameters))
	}
	if cOptions.Query != nil {
		options = append(options, locator.WithQuery(cOptions.Query))
	}
	if cOptions.Header != nil {
		options = append(options, locator.WithHeaders(cOptions.Header))
	}

	aSession := session.New(aComponent.View, session.WithLocatorOptions(options...),
		session.WithAuth(d.auth),
		session.WithRegistry(d.registry),
		session.WithLogger(cOptions.Logger),
		session.WithComponent(aComponent),
		session.WithOperate(d.service.Operate))
	ctx = aSession.Context(ctx, true)
	value, err := d.service.Operate(ctx, aSession, aComponent)
	return value, err
}

// New creates a dispatcher
func New(registry *repository.Registry, auth *auth.Service) contract.Dispatcher {
	return &Dispatcher{
		registry: registry,
		auth:     auth,
		service:  operator.New(),
	}
}
