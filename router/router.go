package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/marshal"
	cusJson "github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/visitor"
	"github.com/viant/toolbox"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type viewHandler func(response http.ResponseWriter, request *http.Request)

const (
	AllowOriginHeader      = "Access-Control-Allow-Origin"
	AllowHeadersHeader     = "Access-Control-Allow-Headers"
	AllowMethodsHeader     = "Access-Control-Allow-Methods"
	AllowCredentialsHeader = "Access-Control-Allow-Credentials"
	ExposeHeadersHeader    = "Access-Control-Expose-Headers"
	MaxAgeHeader           = "Access-Control-Max-Age"

	Separator = " "
)

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
		Cors    *Cors
		Output

		Index Index

		_resource *data.Resource
	}

	Cors struct {
		AllowCredentials *bool
		AllowHeaders     *[]string
		AllowMethods     *[]string
		AllowOrigins     *[]string
		ExposeHeaders    *[]string
		MaxAge           *int64
	}

	Output struct {
		Cardinality data.Cardinality
		CaseFormat  data.CaseFormat
		OmitEmpty   bool

		_marshaller *cusJson.Marshaller
		Style       string //enum Basic, Comprehensice , Status: ok, error, + error with structre
		//TODO add CaseFormat attribute to control output
		//TODO add output key
		//TODO make if output key non empty pass Status, and Error info in the response
	}
)

func (c *Cors) inherit(cors *Cors) {
	if cors == nil {
		return
	}

	if c.ExposeHeaders == nil {
		c.ExposeHeaders = cors.ExposeHeaders
	}

	if c.AllowMethods == nil {
		c.AllowMethods = cors.AllowMethods
	}

	if c.AllowHeaders == nil {
		c.AllowHeaders = cors.AllowHeaders
	}

	if c.AllowOrigins == nil {
		c.AllowOrigins = cors.AllowOrigins
	}

	if c.AllowCredentials == nil {
		c.AllowCredentials = cors.AllowCredentials
	}

	if c.MaxAge == nil {
		c.MaxAge = cors.MaxAge
	}
}

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if r.View.Name == "" {
		r.View.Name = r.View.Ref
	}
	if err := r.View.Init(ctx, resource.Resource); err != nil {
		return err
	}
	if err := r.initVisitor(resource); err != nil {
		return err
	}
	if err := r.Index.Init(r.View); err != nil {
		return err
	}

	if err := r.initCardinality(); err != nil {
		return err
	}

	if err := r.initCaser(); err != nil {
		return err
	}

	r.initCors(resource)

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

func (r *Route) initCardinality() error {
	switch r.Cardinality {
	case data.One, data.Many:
		return nil
	case "":
		r.Cardinality = data.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Cardinality)
	}
}

func (r *Route) initCaser() error {
	if r.CaseFormat == "" {
		r.CaseFormat = data.UpperCamel
	}

	caser, err := r.CaseFormat.Caser()
	if err != nil {
		return err
	}

	marshaller, err := cusJson.New(r.View.Schema.Type(), marshal.Default{
		OmitEmpty:  r.OmitEmpty,
		CaseFormat: caser,
	})

	if err != nil {
		return err
	}

	r._marshaller = marshaller
	return nil
}

func (r *Route) initCors(resource *Resource) {
	if r.Cors == nil {
		r.Cors = resource.Cors
		return
	}

	r.Cors.inherit(resource.Cors)
}

func (i *Index) ViewByPrefix(prefix string) (*data.View, error) {
	view, ok := i._viewsByPrefix[prefix]
	if !ok {
		return nil, fmt.Errorf("not found view with prefix %v", prefix)
	}

	return view, nil
}

func (r *Router) View(name string) (*data.View, error) {
	return r.resource.Resource.View(name)
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
	routings := make([]toolbox.ServiceRouting, 0)

	for i, route := range routes {
		routings = append(routings, toolbox.ServiceRouting{
			URI:        route.URI,
			Handler:    r.viewHandler(routes[i]),
			HTTPMethod: route.Method,
			Parameters: []string{"@httpResponseWriter", "@httpRequest"},
		})

		if route.Cors != nil {
			routings = append(routings, corsRouting(route))
		}
	}

	r.serviceRouter = toolbox.NewServiceRouter(routings...)
}

func corsRouting(route *Route) toolbox.ServiceRouting {
	return toolbox.ServiceRouting{
		URI:        route.URI,
		Handler:    corsHandler(route.Cors),
		HTTPMethod: http.MethodOptions,
		Parameters: []string{"@httpResponseWriter"},
	}
}

func corsHandler(cors *Cors) func(writer http.ResponseWriter) {
	return func(writer http.ResponseWriter) {
		if cors.AllowOrigins != nil {
			writer.Header().Set(AllowOriginHeader, strings.Join(*cors.AllowOrigins, Separator))
		}

		if cors.AllowMethods != nil {
			writer.Header().Set(AllowMethodsHeader, strings.Join(*cors.AllowMethods, Separator))
		}

		if cors.AllowHeaders != nil {
			writer.Header().Set(AllowHeadersHeader, strings.Join(*cors.AllowHeaders, Separator))
		}

		if cors.AllowCredentials != nil {
			writer.Header().Set(AllowCredentialsHeader, strconv.FormatBool(*cors.AllowCredentials))
		}

		if cors.MaxAge != nil {
			writer.Header().Set(MaxAgeHeader, strconv.Itoa(int(*cors.MaxAge)))
		}

		if cors.ExposeHeaders != nil {
			writer.Header().Set(ExposeHeadersHeader, strings.Join(*cors.ExposeHeaders, Separator))
		}

	}
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
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
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
		response.WriteHeader(httpStatus)
		response.Write([]byte(err.Error()))
		return
	}
	response.WriteHeader(httpStatus)
	response.Write(asBytes)
}

func (r *Router) result(route *Route, request *http.Request, destValue reflect.Value) ([]byte, int, error) {
	if route.Cardinality == data.Many {
		asBytes, err := route._marshaller.Marshal(destValue.Elem().Interface())
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
		asBytes, err := route._marshaller.Marshal(route.View.Schema.Slice().ValueAt(slicePtr, 0))
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil

	default:
		return nil, http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", request.RequestURI, sliceSize)
	}
}
