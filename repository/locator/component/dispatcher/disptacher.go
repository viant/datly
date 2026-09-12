package dispatcher

import (
	"context"
	"fmt"
	"net/http"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/reader"
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

// PrepareQuery builds the reader SQL for a route without executing it. It uses
// the same component, locators, authorization, predicates, and selector state
// as normal dispatch.
func (d *Dispatcher) PrepareQuery(ctx context.Context, path *contract.Path, request *http.Request) (*contract.PreparedQuery, error) {
	aComponent, err := d.registry.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	if request == nil {
		return nil, fmt.Errorf("failed to prepare %v %v: request was empty", path.Method, path.URI)
	}
	unmarshal := aComponent.UnmarshalFunc(request)
	options := aComponent.LocatorOptions(request, nil, unmarshal)
	aSession := session.New(aComponent.View, session.WithLocatorOptions(options...),
		session.WithAuth(d.auth),
		session.WithRegistry(d.registry),
		session.WithComponent(aComponent),
		session.WithOperate(d.service.Operate))
	ctx = aSession.Context(ctx, true)
	if err := aSession.ApplyOutputProjection(ctx, aComponent.View); err != nil {
		return nil, err
	}
	if err := aSession.Populate(ctx); err != nil {
		return nil, err
	}
	statelet := aSession.State().Lookup(aComponent.View)
	query, err := reader.NewBuilder().Build(ctx,
		reader.WithBuilderView(aComponent.View),
		reader.WithBuilderStatelet(statelet),
		reader.WithBuilderExclude(false, true))
	if err != nil {
		return nil, err
	}
	return &contract.PreparedQuery{SQL: query.SQL, Args: append([]interface{}{}, query.Args...)}, nil
}

// New creates a dispatcher
func New(registry *repository.Registry, auth *auth.Service) contract.Dispatcher {
	return &Dispatcher{
		registry: registry,
		auth:     auth,
		service:  operator.New(),
	}
}
