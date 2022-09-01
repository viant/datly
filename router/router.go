package router

import (
	"bytes"
	"context"
	"encoding/base64"
	goJson "encoding/json"
	"fmt"
	"github.com/go-playground/validator"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"io"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

//TODO: Add to meta response size
type viewHandler func(response http.ResponseWriter, request *http.Request)

const (
	AllowOriginHeader      = "Access-Control-Allow-Origin"
	AllowHeadersHeader     = "Access-Control-Allow-Headers"
	AllowMethodsHeader     = "Access-Control-Allow-Methods"
	AllowCredentialsHeader = "Access-Control-Allow-Credentials"
	ExposeHeadersHeader    = "Access-Control-Expose-Headers"
	MaxAgeHeader           = "Access-Control-Max-Age"
	Separator              = ", "
)

var errorFilters = json.NewFilters(&json.FilterEntry{
	Fields: []string{"Status", "Message"},
})
var debugEnabled = os.Getenv("DATLY_DEBUG") != ""

type (
	Router struct {
		resource *Resource
		index    map[string][]int
		routes   Routes
		Matcher  *Matcher
	}

	BytesReadCloser struct {
		bytes *bytes.Buffer
	}

	ApiPrefix string
)

func (b *BytesReadCloser) Read(p []byte) (int, error) {
	return b.bytes.Read(p)
}

func (b *BytesReadCloser) Close() error {
	return nil
}

func (r *Router) View(name string) (*view.View, error) {
	return r.resource.Resource.View(name)
}

func (r *Router) Handle(response http.ResponseWriter, request *http.Request) error {
	route, err := r.Matcher.Match(request.Method, request.URL.Path)
	if err != nil {
		return err
	}

	if apiKey := route.APIKey; apiKey != nil {
		key := request.Header.Get(apiKey.Header)
		if key != apiKey.Value {
			response.WriteHeader(http.StatusUnauthorized)
			return nil
		}
	}
	return r.handleRoute(response, request, route)
}

func (r *Router) handleRoute(response http.ResponseWriter, request *http.Request, route *Route) error {
	if request.Method == http.MethodOptions {
		corsHandler(route.Cors)(response)
		return nil
	}

	switch route.Service {
	case ReaderServiceType:
		r.viewHandler(route)(response, request)
		return nil
	case ExecutorServiceType:
		r.executorHandler(route)(response, request)
		return nil
	}

	return fmt.Errorf("unsupported service operation %v", request.Method)
}

func New(resource *Resource, options ...interface{}) *Router {
	var apiPrefix string
	for _, option := range options {
		switch actual := option.(type) {
		case ApiPrefix:
			apiPrefix = string(actual)
		}
	}

	router := &Router{
		resource: resource,
		index:    map[string][]int{},
		routes:   resource.Routes,
	}

	router.Init(resource.Routes, apiPrefix)

	return router
}

func (r *Router) Init(routes Routes, apiPrefix string) {
	for _, route := range routes {
		r.normalizeRouteURI(apiPrefix, route)

		route._resource = r.resource.Resource
	}

	r.indexRoutes()
	r.initMatcher()
}

func corsHandler(cors *Cors) func(writer http.ResponseWriter) {
	return func(writer http.ResponseWriter) {
		enableCors(writer, cors, true)
	}
}

func enableCors(writer http.ResponseWriter, cors *Cors, allHeaders bool) {
	if cors == nil {
		return
	}

	if cors.AllowOrigins != nil {
		writer.Header().Set(AllowOriginHeader, strings.Join(*cors.AllowOrigins, Separator))
	}

	if cors.AllowMethods != nil && allHeaders {
		writer.Header().Set(AllowMethodsHeader, strings.Join(*cors.AllowMethods, Separator))
	}

	if cors.AllowHeaders != nil && allHeaders {
		writer.Header().Set(AllowHeadersHeader, strings.Join(*cors.AllowHeaders, Separator))
	}

	if cors.AllowCredentials != nil && allHeaders {
		writer.Header().Set(AllowCredentialsHeader, strconv.FormatBool(*cors.AllowCredentials))
	}

	if cors.MaxAge != nil && allHeaders {
		writer.Header().Set(MaxAgeHeader, strconv.Itoa(int(*cors.MaxAge)))
	}

	if cors.ExposeHeaders != nil && allHeaders {
		writer.Header().Set(ExposeHeadersHeader, strings.Join(*cors.ExposeHeaders, Separator))
	}
}

func (r *Router) Serve(serverPath string) error {
	return http.ListenAndServe(serverPath, r)
}

func (r *Router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	route, err := r.Matcher.Match(request.Method, request.URL.Path)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}

	err = r.handleRoute(writer, request, route)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		if route.Cors != nil {
			enableCors(response, route.Cors, false)
		}

		if route.EnableAudit {
			r.logAudit(request)
		}

		if !r.runBeforeFetch(response, request, route) {
			return
		}

		ctx := context.Background()
		selectors, err := CreateSelectorsFromRoute(ctx, route, request, route.Index._viewDetails...)
		if err != nil {
			status := http.StatusBadRequest
			if route.ParamStatusError != nil && (*route.ParamStatusError%100 >= 4) {
				status = *route.ParamStatusError
			}

			r.writeErr(response, route, err, status)
			return
		}

		cacheEntry, err := r.cacheEntry(ctx, route, selectors)
		if err != nil {
			r.writeErr(response, route, err, http.StatusInternalServerError)
		}

		if cacheEntry != nil && cacheEntry.Has() {
			r.writeResponse(ctx, route, response, request, cacheEntry)
			return
		}

		r.writeResponseWithErrorHandler(response, request, ctx, route, selectors, cacheEntry)
	}
}

func (r *Router) writeResponseWithErrorHandler(response http.ResponseWriter, request *http.Request, ctx context.Context, route *Route, selectors *view.Selectors, cacheEntry *cache.Entry) {
	httpCode, err := r.readAndWriteResponse(response, request, ctx, route, selectors, cacheEntry)
	if err != nil {
		r.writeErr(response, route, err, httpCode)
	}
}

func (r *Router) readAndWriteResponse(response http.ResponseWriter, request *http.Request, ctx context.Context, route *Route, selectors *view.Selectors, entry *cache.Entry) (statusCode int, err error) {
	rValue, viewMeta, err := r.readValue(route, selectors)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	if !r.runAfterFetch(response, request, route, rValue.Interface()) {
		return -1, nil
	}

	resultMarshalled, statusCode, err := r.marshalResult(route, request, selectors, rValue, viewMeta)
	if err != nil {
		return statusCode, err
	}

	payloadReader, err := r.compressIfNeeded(resultMarshalled, route)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	templateMeta := route.View.Template.Meta
	if templateMeta != nil && templateMeta.Kind == view.HeaderTemplateMetaKind && viewMeta != nil {
		data, err := goJson.Marshal(viewMeta)
		if err != nil {
			return http.StatusInternalServerError, err
		}

		payloadReader.AddHeader(templateMeta.Name, string(data))
	}

	if entry != nil {
		r.updateCache(ctx, route, entry, payloadReader)
	}

	r.writeResponse(ctx, route, response, request, payloadReader)
	return -1, nil
}

func (r *Router) readValue(route *Route, selectors *view.Selectors) (reflect.Value, interface{}, error) {
	destValue := reflect.New(route.View.Schema.SliceType())
	dest := destValue.Interface()

	session := reader.NewSession(dest, route.View)
	session.Selectors = selectors
	if err := reader.New().Read(context.TODO(), session); err != nil {
		return destValue, nil, err
	}

	if route.EnableAudit {
		r.logMetrics(route.URI, session.Metrics)
	}

	return destValue, session.ViewMeta, nil
}

func (r *Router) updateCache(ctx context.Context, route *Route, cacheEntry *cache.Entry, response *RequestDataReader) {
	if !debugEnabled {
		go r.putCache(ctx, route, cacheEntry, response)
		return
	}

	r.putCache(ctx, route, cacheEntry, response)
}

func (r *Router) cacheEntry(ctx context.Context, route *Route, selectors *view.Selectors) (*cache.Entry, error) {
	if route.Cache == nil {
		return nil, nil
	}

	cacheEntry, err := r.createCacheEntry(ctx, route, selectors)
	if err != nil {
		return nil, err
	}

	return cacheEntry, nil
}

func (r *Router) putCache(ctx context.Context, route *Route, cacheEntry *cache.Entry, payloadReader *RequestDataReader) {
	_ = route.Cache.Put(ctx, cacheEntry, payloadReader.buffer.Bytes(), payloadReader.CompressionType(), payloadReader.Headers())
}

func (r *Router) runBeforeFetch(response http.ResponseWriter, request *http.Request, route *Route) (shouldContinue bool) {
	if actual, ok := route.Visitor.Visitor().(codec.BeforeFetcher); ok {
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
	if actual, ok := route.Visitor.Visitor().(codec.AfterFetcher); ok {
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

func (r *Router) marshalResult(route *Route, request *http.Request, selectors *view.Selectors, destValue reflect.Value, viewMeta interface{}) (result []byte, statusCode int, err error) {
	filters, err := r.buildJsonFilters(route, selectors)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	payload, httpStatus, err := r.result(route, request, destValue, filters, viewMeta)
	if err != nil {
		return nil, httpStatus, err
	}
	return payload, httpStatus, nil
}

func (r *Router) inAWS() bool {
	scheme := url.Scheme(r.resource.SourceURL, "s3")
	return scheme == "s3"
}

func (r *Router) result(route *Route, request *http.Request, destValue reflect.Value, filters *json.Filters, meta interface{}) ([]byte, int, error) {
	if route.Cardinality == view.Many {
		result := r.wrapWithResponseIfNeeded(destValue.Elem().Interface(), route, meta)
		asBytes, err := route._marshaller.Marshal(result, filters)
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
		result := r.wrapWithResponseIfNeeded(route.View.Schema.Slice().ValueAt(slicePtr, 0), route, meta)
		asBytes, err := route._marshaller.Marshal(result, filters)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil

	default:
		return nil, http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", request.RequestURI, sliceSize)
	}
}

func (r *Router) buildJsonFilters(route *Route, selectors *view.Selectors) (*json.Filters, error) {
	entries := make([]*json.FilterEntry, 0)

	selectors.Lock()
	defer selectors.Unlock()
	for viewName, selector := range selectors.Index {
		if len(selector.Columns) == 0 {
			continue
		}

		var path string
		viewByName, ok := route.Index.viewByName(viewName)
		if !ok {
			path = ""
		} else {
			path = viewByName.Path
		}

		fields := make([]string, len(selector.Fields))
		for i := range selector.Fields {
			fields[i] = selector.Fields[i]
		}

		entries = append(entries, &json.FilterEntry{
			Path:   path,
			Fields: fields,
		})

	}

	return json.NewFilters(entries...), nil
}

func (r *Router) writeErr(w http.ResponseWriter, route *Route, err error, statusCode int) {
	statusCode, err = r.normalizeErr(err, statusCode)
	if route._responseSetter == nil {
		errAsBytes, marshalErr := goJson.Marshal(err)
		if marshalErr != nil {
			w.Write([]byte("could not parse error message"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(statusCode)
		w.Write(errAsBytes)
		return
	}

	response := reflect.New(route._responseSetter.rType)
	r.setResponseStatus(route, response, ResponseStatus{
		Status:  "error",
		Message: err,
	})

	asBytes, marErr := route._marshaller.Marshal(response.Elem().Interface(), errorFilters)
	if marErr != nil {
		w.Write(asBytes)
		w.WriteHeader(statusCode)
		return
	}

	w.Write(asBytes)
	w.WriteHeader(statusCode)
}

func (r *Router) setResponseStatus(route *Route, response reflect.Value, responseStatus ResponseStatus) {
	route._responseSetter.statusField.SetValue(unsafe.Pointer(response.Pointer()), responseStatus)
}

func (r *Router) wrapWithResponseIfNeeded(response interface{}, route *Route, viewMeta interface{}) interface{} {
	if route._responseSetter == nil {
		return response
	}

	newResponse := reflect.New(route._responseSetter.rType)
	responseBodyPtr := unsafe.Pointer(newResponse.Pointer())
	route._responseSetter.bodyField.SetValue(responseBodyPtr, response)
	if route._responseSetter.pageField != nil && viewMeta != nil {
		route._responseSetter.pageField.SetValue(responseBodyPtr, viewMeta)
	}

	r.setResponseStatus(route, newResponse, ResponseStatus{Status: "ok"})
	return newResponse.Elem().Interface()
}

func (r *Router) createCacheEntry(ctx context.Context, route *Route, selectors *view.Selectors) (*cache.Entry, error) {
	selectors.RWMutex.RLock()
	defer selectors.RWMutex.RUnlock()

	selectorSlice := make([]*view.Selector, len(selectors.Index))
	for viewName := range selectors.Index {
		index, _ := route.viewIndex(viewName)
		selectorSlice[index] = selectors.Index[viewName]
	}
	marshalled, err := goJson.Marshal(selectorSlice)
	if err != nil {
		return nil, err
	}

	return route.Cache.Get(ctx, marshalled, route.View.Name)
}

func (r *Router) normalizeErr(err error, statusCode int) (int, error) {
	switch actual := err.(type) {
	case *Errors:
		for _, anError := range actual.Errors {
			switch childErr := anError.Err.(type) {
			case validator.ValidationErrors:
				anError.Object = NewParamErrors(childErr)
				anError.Message = childErr.Error()
			case *Errors:
				_, anError.Object = r.normalizeErr(anError.Err, statusCode)
			case *JSONError:
				anError.Object = childErr
			default:
				anError.Message = anError.Error()
			}
		}

		if actual.status != 0 {
			statusCode = actual.status
		}

		return statusCode, err
	}

	return statusCode, &Error{
		Message: err.Error(),
	}
}

func (r *Router) indexRoutes() {
	for i, route := range r.routes {
		methods, _ := r.index[route.URI]
		methods = append(methods, i)
		r.index[route.URI] = methods
	}
}

func (r *Router) ApiPrefix() string {
	return r.resource.APIURI
}

func (r *Router) Routes(route string) []*Route {
	if route == "" {
		return r.routes
	}

	uriRoutes, ok := r.index[route]
	if !ok {
		return []*Route{}
	}

	routes := make([]*Route, len(uriRoutes))
	for i, routeIndex := range uriRoutes {
		routes[i] = r.routes[routeIndex]
	}

	return routes
}

func (r *Router) writeResponse(ctx context.Context, route *Route, response http.ResponseWriter, request *http.Request, payloadReader PayloadReader) {
	defer payloadReader.Close()

	redirected, err := r.redirectIfNeeded(ctx, route, response, request, payloadReader)
	if redirected {
		return
	}

	if err != nil {
		r.writeErr(response, route, err, http.StatusInternalServerError)
		return
	}

	response.Header().Add(content.Type, ContentTypeJSON)
	response.Header().Add(content.Type, CharsetUTF8)
	response.Header().Add(ContentLength, strconv.Itoa(payloadReader.Size()))
	for key, value := range payloadReader.Headers() {
		response.Header().Add(key, value[0])
	}

	compressionType := payloadReader.CompressionType()
	if compressionType != "" {
		response.Header().Set(content.Encoding, compressionType)
	}

	response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(response, payloadReader)
}

func (r *Router) redirectIfNeeded(ctx context.Context, route *Route, response http.ResponseWriter, request *http.Request, payloadReader PayloadReader) (redirected bool, err error) {
	redirect := r.resource.Redirect
	if redirect == nil {
		return false, nil
	}

	if redirect.MinSizeKb*1024 > payloadReader.Size() {
		return false, nil
	}

	preSign, err := redirect.Apply(ctx, route.View.Name, payloadReader)
	if err != nil {
		return false, err
	}

	http.Redirect(response, request, preSign.URL, http.StatusMovedPermanently)
	return true, nil
}

func (r *Router) compressIfNeeded(marshalled []byte, route *Route) (*RequestDataReader, error) {
	compression := route.Compression
	if compression == nil || (compression.MinSizeKb > 0 && len(marshalled) <= compression.MinSizeKb*1024) {
		return NewBytesReader(marshalled, ""), nil
	}

	buffer, err := Compress(bytes.NewReader(marshalled))
	if err != nil {
		return nil, err
	}

	payloadSize := buffer.Len()
	if r.inAWS() {
		payloadSize = base64.StdEncoding.EncodedLen(payloadSize)
	}

	return AsBytesReader(buffer, EncodingGzip, payloadSize), nil
}

func (r *Router) logAudit(request *http.Request) {
	headers := request.Header.Clone()
	if len(headers.Get("Authorization")) > 0 {
		headers.Set("Authorization", "***")
	}

	asBytes, _ := goJson.Marshal(Audit{
		URL:     request.RequestURI,
		Headers: headers,
	})

	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) logMetrics(URI string, metrics []*reader.Metric) {
	asBytes, _ := goJson.Marshal(struct {
		URI     string
		Metrics []*reader.Metric
	}{URI: URI, Metrics: metrics})

	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) initMatcher() {
	r.Matcher = NewMatcher(r.routes)
}

func (r *Router) normalizeRouteURI(prefix string, route *Route) {
	if prefix == "" {
		return
	}

	if strings.HasPrefix(route.URI, prefix) {
		return
	}

	if prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}

	URI := route.URI
	if URI != "" && URI[len(URI)-1] == '/' {
		URI = URI[:len(URI)-1]
	}

	route.URI = path.Join(prefix, URI)
}
