package dispatcher

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/service/dispatcher"
	"github.com/viant/datly/service/session"
	"net/http"
)

type Dispatcher struct {
	registry *repository.Registry
	service  *dispatcher.Service
}

func (d *Dispatcher) Dispatch(ctx context.Context, path *component.Path, request *http.Request) (interface{}, error) {
	//TODO maybe extract and pass session cache value
	aComponent, err := d.registry.Lookup(path)
	if err != nil {
		return nil, err
	}
	unmarshal := aComponent.UnmarshalFunc(request)
	aSession := session.New(aComponent.View, session.WithLocatorOptions(aComponent.LocatorOptions(request, unmarshal)...))
	if err = aSession.Populate(ctx); err != nil {
		return nil, err
	}
	value, err := d.service.Dispatch(ctx, aComponent, aSession)

	fmt.Printf("%T %+v %v\n ", value, value, err)
	return value, err
}

// New creates a dispatcher
func New(registry *repository.Registry) *Dispatcher {
	return &Dispatcher{
		registry: registry,
		service:  &dispatcher.Service{},
	}
}
