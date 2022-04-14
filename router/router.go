package router

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/reader"
	"github.com/viant/toolbox"
	"net/http"
	"reflect"
	"strings"
)

type viewHandler func(response http.ResponseWriter, request *http.Request)

type (
	Routes []*Route
	Route  struct {
		URI    string
		Method string
		View   *data.View
	}

	Router struct {
		*Resource
		serviceRouter *toolbox.ServiceRouter
	}
)

func (r *Router) Handle(response http.ResponseWriter, request *http.Request) error {
	if err := r.serviceRouter.Route(response, request); err != nil {
		return err
	}

	return nil
}

func New(resource *Resource) *Router {
	router := &Router{
		Resource: resource,
	}

	router.Init(resource.Routes)

	return router
}

func (r *Router) Init(routes Routes) {
	r.initServiceRouter(routes)
}

func (r *Router) initServiceRouter(routes Routes) {
	routings := make([]toolbox.ServiceRouting, len(routes))
	for i, route := range routes {
		routings[i] = toolbox.ServiceRouting{
			URI:        route.URI,
			Handler:    r.viewHandler(routes[i]),
			HTTPMethod: route.Method,
			Parameters: []string{"@httpResponseWriter", "@httpRequest"},
		}
	}

	r.serviceRouter = toolbox.NewServiceRouter(routings...)
}

func (r *Router) Serve(serverPath string) error {
	return http.ListenAndServe(serverPath, r)
}

func (r *Router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	r.serviceRouter.Route(writer, request)
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		destValue := reflect.New(route.View.Schema.SliceType())
		dest := destValue.Interface()
		session := reader.NewSession(dest, route.View)

		selectors, err := r.createSelectors(route, request)
		session.Selectors = selectors

		service := reader.New()
		if err := service.Read(context.TODO(), session); err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return
		}

		asBytes, err := json.Marshal(destValue.Elem().Interface())
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			response.Write([]byte(err.Error()))
			return
		}

		response.WriteHeader(http.StatusOK)
		response.Write(asBytes)
	}
}

func (r *Router) createSelectors(route *Route, request *http.Request) (data.Selectors, error) {
	selectors := data.Selectors{}

	var err error
	if err = r.buildColumns(&selectors, route, request); err != nil {
		return nil, err
	}

	if err = r.buildOffset(&selectors, route, request); err != nil {
		return nil, err
	}

	return selectors, nil
}

func (r *Router) buildColumns(s *data.Selectors, route *Route, request *http.Request) error {
	fields := toolbox.QueryValue(request.URL, string(Fields), "")
	if fields == "" {
		return nil
	}

	var selector *data.Selector
	for _, field := range strings.Split(fields, "|") {
		viewField := strings.Split(field, ".")
		if len(viewField) < 2 {
			selector = s.GetOrCreate(route.View.Name)
			selector.Columns = append(selector.Columns, field)
			continue
		}

		if len(viewField) > 2 {
			return fmt.Errorf("unsupported field format, supported formats: [fieldName] || [prefix.FieldName]")
		}

		viewName, ok := r.ViewPrefix[viewField[0]]
		if !ok {
			return fmt.Errorf("unspecified prefix %v", viewField[0])
		}

		selector = s.GetOrCreate(viewName)
		selector.Columns = append(selector.Columns, viewField[1])
	}

	return nil
}

func (r *Router) buildOffset(s *data.Selectors, route *Route, request *http.Request) error {
	fields := toolbox.QueryValue(request.URL, string(Fields), "")
	if fields == "" {
		return nil
	}

	var selector *data.Selector
	for _, field := range strings.Split(fields, "|") {
		viewField := strings.Split(field, ".")
		if len(viewField) < 2 {
			selector = s.GetOrCreate(route.View.Name)
			selector.Columns = append(selector.Columns, field)
			continue
		}

		if len(viewField) > 2 {
			return fmt.Errorf("unsupported field format, supported formats: [fieldName] || [prefix.FieldName]")
		}

		viewName, ok := r.ViewPrefix[viewField[0]]
		if !ok {
			return fmt.Errorf("unspecified prefix %v", viewField[0])
		}

		selector = s.GetOrCreate(viewName)
		selector.Columns = append(selector.Columns, viewField[1])
	}

	return nil
}
