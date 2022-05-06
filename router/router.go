package router

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/visitor"
	"github.com/viant/toolbox"
	"net/http"
	"reflect"
	"unsafe"
)

type viewHandler func(response http.ResponseWriter, request *http.Request)

type (
	Router struct {
		resource      *Resource
		serviceRouter *toolbox.ServiceRouter
	}

	Routes []*Route
	Route  struct {
		Visitor *visitor.Visitor
		URI     string
		Method  string
		View    *data.View
		Output

		Index Index

		_resource *data.Resource
	}

	Output struct {
		//TODO rename ReturnSingle to Cardinality
		ReturnSingle bool
		Style        string //enum Basic, Comprehensice , Status: ok, error, + error with structre
		//TODO add CaseFormat attribute to control output
		//TODO add output key
		//TODO make if output key non empty pass Status, and Error info in the response
	}
)

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if err := r.View.Init(ctx, resource.Resource); err != nil {
		return err
	}
	if err := r.initVisitor(resource); err != nil {
		return err
	}
	if err := r.Index.Init(r.View); err != nil {
		return err
	}

	return nil
}

func (r *Route) initVisitor(resource *Resource) error {
	if r.Visitor == nil {
		r.Visitor = &visitor.Visitor{}
		return nil
	}

	if r.Visitor.Reference.Ref != "" {
		refVisitor, err := resource._visitors.Lookup(r.Visitor.Ref)
		if err != nil {
			return err
		}

		r.Visitor.Inherit(refVisitor)
	}

	return nil
}

func (i *Index) ViewByPrefix(prefix string) (*data.View, error) {
	view, ok := i._viewsByPrefix[prefix]
	if !ok {
		return nil, fmt.Errorf("not found view with prefix %v", prefix)
	}

	return view, nil
}

func (r *Router) Handle(response http.ResponseWriter, request *http.Request) error {
	if err := r.serviceRouter.Route(response, request); err != nil {
		return err
	}

	return nil
}

func New(resource *Resource) *Router {
	router := &Router{
		resource: resource,
	}

	router.Init(resource.Routes)

	return router
}

func (r *Router) Init(routes Routes) {
	for _, route := range routes {
		route._resource = r.resource.Resource
	}

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
	err := r.serviceRouter.Route(writer, request)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		if !r.runBeforeFetch(response, request, route) {
			return
		}

		destValue := reflect.New(route.View.Schema.SliceType())
		dest := destValue.Interface()
		session := reader.NewSession(dest, route.View)

		ctx := context.Background()

		selectors, err := CreateSelectorsFromRoute(ctx, route, request, route.Index._views...)
		if err != nil {
			response.Write([]byte(err.Error()))
			response.WriteHeader(http.StatusBadRequest)
			return
		}

		session.Selectors = selectors

		if err := reader.New().Read(context.TODO(), session); err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return
		}

		if !r.runAfterFetch(response, request, route, dest) {
			return
		}

		r.writeResponse(route, request, response, destValue)
	}
}

func (r *Router) runBeforeFetch(response http.ResponseWriter, request *http.Request, route *Route) (shouldContinue bool) {
	if actual, ok := route.Visitor.Visitor().(visitor.BeforeFetcher); ok {
		closed, err := actual.BeforeFetch(response, request)
		if closed {
			return false
		}

		if err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return false
		}
	}
	return true
}

func (r *Router) runAfterFetch(response http.ResponseWriter, request *http.Request, route *Route, dest interface{}) (shouldContinue bool) {
	if actual, ok := route.Visitor.Visitor().(visitor.AfterFetcher); ok {
		responseClosed, err := actual.AfterFetch(dest, response, request)
		if responseClosed {
			return false
		}

		if err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return false
		}
	}

	return true
}

func (r *Router) writeResponse(route *Route, request *http.Request, response http.ResponseWriter, destValue reflect.Value) {
	asBytes, httpStatus, err := r.result(route, request, destValue)

	if err != nil {
		response.Write([]byte(err.Error()))
		response.WriteHeader(httpStatus)
		return
	}

	response.Write(asBytes)
	response.WriteHeader(httpStatus)
}

func (r *Router) result(route *Route, request *http.Request, destValue reflect.Value) ([]byte, int, error) {
	if !route.ReturnSingle {
		asBytes, err := json.Marshal(destValue.Elem().Interface())
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil
	}

	slicePtr := unsafe.Pointer(destValue.Pointer())
	sliceSize := route.View.Schema.Slice().Len(slicePtr)
	switch sliceSize {
	case 0:
		return nil, http.StatusNotFound, nil
	case 1:
		asBytes, err := json.Marshal(route.View.Schema.Slice().ValuePointerAt(slicePtr, 0))
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil

	default:
		return nil, http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", request.RequestURI, sliceSize)
	}
}
