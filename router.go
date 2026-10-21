package datly

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
)

type Handler[T any] func(ctx context.Context, service T, request *http.Request, injector hstate.Injector, extra ...OperateOption) (interface{}, error)

type Route[T any] struct {
	dao       *Service
	handler   Handler[T]
	service   T
	path      *contract.Path
	component *repository.Component
}

func (r Route[T]) ensureComponent(ctx context.Context) (*repository.Component, error) {
	if r.component == nil {
		var err error
		r.component, err = r.dao.repository.Registry().Lookup(ctx, r.path)
		if err != nil {
			return nil, err
		}
	}
	return r.component, nil
}

func (r Route[T]) Run(ctx context.Context, writer http.ResponseWriter, request *http.Request) error {
	marshaller, contentType, _, err := r.dao.getMarshaller(request, r.component)
	if err != nil {
		return fmt.Errorf("failed to lookup marshaller: %w", err)
	}
	injector, err := r.dao.GetInjector(request, r.component)
	if err != nil {
		return fmt.Errorf("failed to lookup injector: %w", err)
	}
	selectors := []*hstate.NamedQuerySelector{}
	values := request.URL.Query()
	if page := values.Get("page"); page != "" {
		selector := &hstate.NamedQuerySelector{Name: r.component.View.Name}
		selector.Page, _ = strconv.Atoi(page)
		selectors = append(selectors, selector)
	}
	result, err := r.handler(ctx, r.service, request, injector, WithSessionOptions(WithRequest(request), WithQuerySelectors(selectors...)))
	var data []byte
	if err != nil {
		rErr, ok := err.(*response.Error)
		if !ok {
			rErr = response.NewError(http.StatusInternalServerError, err.Error())
		}
		data, err = marshaller(rErr)
	} else {
		data, err = marshaller(result)
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return nil
	}
	statusCode := http.StatusOK
	statusCoder, ok := result.(response.StatusCoder)
	if ok {
		statusCode = statusCoder.StatusCode()
	}

	writer.Header().Set("Content-Type", contentType)
	writer.WriteHeader(statusCode)
	_, err = writer.Write(data)
	return err
}

func newRoute[T any](dao *Service, path *contract.Path, component *repository.Component, service T, handler Handler[T]) *Route[T] {
	return &Route[T]{path: path, handler: handler, dao: dao, component: component, service: service}
}

type Router[T any] struct {
	registry map[string]*Route[T]
	dao      *Service
	service  T
}

type routeNotFound struct {
	error
}

// IsRouteNotFound checks if error is route not found
func IsRouteNotFound(err error) bool {
	_, ok := err.(*routeNotFound)
	return ok
}

func (r *Router[T]) Run(writer http.ResponseWriter, request *http.Request) error {
	aPath := contract.NewPath(request.Method, request.URL.Path)
	component, err := r.dao.repository.Registry().Lookup(request.Context(), aPath)
	if err != nil {
		fmt.Println(err)
		return &routeNotFound{err}
	}
	route, ok := r.registry[component.Path.Key()]
	if !ok {
		return &routeNotFound{errors.New("route not found")}
	}
	return route.Run(request.Context(), writer, request)
}

func (r *Router[T]) Register(ctx context.Context, path *contract.Path, handler Handler[T]) error {
	component, err := r.dao.repository.Registry().Lookup(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to lookup component: %w for path: %+v", err, path)
	}
	route := newRoute[T](r.dao, path, component, r.service, handler)
	r.registry[path.Key()] = route
	return nil
}

func NewRouter[T any](dao *Service, service T) *Router[T] {
	return &Router[T]{registry: make(map[string]*Route[T]), dao: dao, service: service}
}

type BodyEnvelope[T any] struct {
	Body T `parameter:",kind=body"`
}
