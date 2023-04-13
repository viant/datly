package router

import (
	"bytes"
	"context"
	"encoding/base64"
	goJson "encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/config"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/govalidator"
	"github.com/viant/scy/auth/jwt"
	svalidator "github.com/viant/sqlx/io/validator"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
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

	DatlyRequestMetricsHeader      = "Datly-Show-Metrics"
	DatlyInfoHeaderValue           = "info"
	DatlyDebugHeaderValue          = "debug"
	DatlyRequestDisableCacheHeader = "Datly-Disable-Cache"
	DatlyResponseHeaderMetrics     = "Datly-Metrics"

	DatlyServiceTimeHeader = "Datly-Service-Time"
	DatlyServiceInitHeader = "Datly-Service-Init"
)

var debugEnabled = os.Getenv("DATLY_DEBUG") != ""
var strErrType = reflect.TypeOf(fmt.Errorf(""))

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

	ReaderSession struct {
		RequestParams *RequestParams
		Route         *Route
		Request       *http.Request
		Response      http.ResponseWriter
		Selectors     *view.Selectors
	}
)

func (s *ReaderSession) IsMetricsEnabled() bool {
	return s.Route.DebugKind == view.MetaTypeHeader || (s.IsMetricInfo() || s.IsMetricDebug())
}

func (r *Route) IsMetricsEnabled(req *http.Request) bool {
	return r.IsMetricInfo(req) || r.IsMetricDebug(req)
}

func (r *Route) IsMetricInfo(req *http.Request) bool {
	if !r.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == DatlyInfoHeaderValue
}

func (r *Route) IsMetricDebug(req *http.Request) bool {
	if !r.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == DatlyDebugHeaderValue
}

func (s *ReaderSession) IsMetricDebug() bool {
	return s.Route.IsMetricDebug(s.Request)
}

func (s *ReaderSession) IsMetricInfo() bool {
	return s.Route.IsMetricInfo(s.Request)
}

func (s *ReaderSession) IsCacheDisabled() bool {
	if s.Route.EnableDebug == nil {
		return false
	}

	return (*s.Route.EnableDebug) && (s.Request.Header.Get(DatlyRequestDisableCacheHeader) != "" || s.Request.Header.Get(strings.ToLower(DatlyRequestDisableCacheHeader)) != "")
}

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
	if r.resource.Interceptor != nil {
		_, err := r.resource.Interceptor.Intercept(request)
		if err != nil {
			code, message := httputils.BuildErrorResponse(err)
			response.WriteHeader(code)
			response.Write([]byte(message))
			return nil
		}
	}

	route, err := r.Matcher.MatchOneRoute(request.Method, request.URL.Path)
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
	return r.HandleRoute(response, request, route)
}

func (r *Router) HandleRoute(response http.ResponseWriter, request *http.Request, route *Route) error {
	if request.Method == http.MethodOptions {
		corsHandler(request, route.Cors)(response)
		return nil
	}

	switch route.Service {
	case ReaderServiceType:
		r.prepareViewHandler(response, request, route)
		r.viewHandler(route)(response, request)
		return nil
	case ExecutorServiceType:
		r.prepareViewHandler(response, request, route)
		r.executorHandler(route)(response, request)
		return nil
	}

	return fmt.Errorf("unsupported service operation %v", request.Method)
}

func (r *Router) prepareViewHandler(response http.ResponseWriter, request *http.Request, route *Route) {
	if route.Cors != nil {
		enableCors(response, request, route.Cors, false)
	}

	if route.EnableAudit {
		r.logAudit(request, response, route)
	}
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

func (r *Router) Init(routes Routes, apiPrefix string) error {
	for _, route := range routes {
		route.URI = r.normalizeURI(apiPrefix, route.URI)

		route._resource = r.resource.Resource
	}

	r.indexRoutes()
	r.initMatcher()

	if r.resource.URL != "" {
		r.resource.URL = r.normalizeURI(apiPrefix, r.resource.URL)
	}

	if r.resource.Interceptor != nil {
		if err := r.resource.Interceptor.init(r.resource.URL); err != nil {
			return err
		}
	}

	return nil
}

func corsHandler(request *http.Request, cors *Cors) func(writer http.ResponseWriter) {
	return func(writer http.ResponseWriter) {
		enableCors(writer, request, cors, true)
	}
}

func enableCors(writer http.ResponseWriter, request *http.Request, cors *Cors, allHeaders bool) {

	if cors == nil {
		return
	}

	origins := request.Header["Origin"]
	origin := ""
	if len(origins) > 0 {
		origin = origins[0]
	}
	if origin == "" {
		writer.Header().Set(AllowOriginHeader, "*")
	} else {
		writer.Header().Set(AllowOriginHeader, origin)
	}

	if cors.AllowMethods != nil && allHeaders {
		writer.Header().Set(AllowMethodsHeader, request.Method)
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
	route, err := r.Matcher.MatchOneRoute(request.Method, request.URL.Path)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}

	err = r.HandleRoute(writer, request, route)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {

		if !r.runBeforeFetchIfNeeded(response, request, route) {
			return
		}

		ctx := context.Background()
		session, httpErrStatus, err := r.buildSession(ctx, response, request, route)
		if httpErrStatus >= http.StatusBadRequest {
			r.writeErr(response, route, err, httpErrStatus)
			return
		}

		if err != nil {
			status := http.StatusBadRequest
			if route.ParamStatusError != nil && (*route.ParamStatusError%100 >= 4) {
				status = *route.ParamStatusError
			}

			r.writeErr(session.Response, session.Route, err, status)
			return
		}

		payloadReader, err := r.payloadReader(ctx, session, route)
		if err != nil {
			code, _ := httputils.BuildErrorResponse(err)
			r.writeErr(response, route, err, code)
			return
		}

		r.writeResponse(ctx, session, payloadReader)
	}
}

func (r *Router) buildSession(ctx context.Context, response http.ResponseWriter, request *http.Request, route *Route) (*ReaderSession, int, error) {
	requestParams, err := NewRequestParameters(request, route)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if route.CSV == nil && requestParams.OutputFormat == CSVFormat {
		return nil, http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output CSV config?)", CSVFormat))
	}

	if route.TabularJSON == nil && route.DateFormat == TabularJSONQueryFormat {
		return nil, http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output DataFormat config?)", TabularJSONFormat))
	}

	selectors, _, err := CreateSelectorsFromRoute(ctx, route, request, requestParams, route.Index._viewDetails...)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return &ReaderSession{
		RequestParams: requestParams,
		Route:         route,
		Request:       request,
		Response:      response,
		Selectors:     selectors,
	}, http.StatusOK, nil
}

func UnsupportedFormatErr(format string) error {
	return fmt.Errorf("unsupported output format %v", format)
}

func (r *Router) readResponse(ctx context.Context, session *ReaderSession) (PayloadReader, error) {
	rValue, viewMeta, readerStats, err := r.readValue(session)

	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	if !r.runAfterFetchIfNeeded(session, rValue.Interface()) {
		return nil, nil
	}

	resultMarshalled, statusCode, err := r.marshalResult(session, rValue, viewMeta, readerStats)
	if err != nil {
		return nil, httputils.NewHttpMessageError(statusCode, err)
	}

	payloadReader, err := r.compressIfNeeded(resultMarshalled, session.Route)
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	templateMeta := session.Route.View.Template.Meta
	if templateMeta != nil && templateMeta.Kind == view.MetaTypeHeader && viewMeta != nil {
		data, err := goJson.Marshal(viewMeta)
		if err != nil {
			return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
		}

		payloadReader.AddHeader(templateMeta.Name, string(data))
	}

	for _, stat := range readerStats {
		marshal, err := goJson.Marshal(stat)
		if err != nil {
			continue
		}

		payloadReader.AddHeader(DatlyResponseHeaderMetrics+"-"+stat.Name(), string(marshal))
	}

	return payloadReader, nil
}

func (r *Router) readValue(readerSession *ReaderSession) (reflect.Value, interface{}, []*reader.Info, error) {
	destValue := reflect.New(readerSession.Route.View.Schema.SliceType())
	dest := destValue.Interface()

	session := reader.NewSession(dest, readerSession.Route.View)
	session.CacheDisabled = readerSession.IsCacheDisabled()
	session.IncludeSQL = readerSession.IsMetricDebug()

	session.Selectors = readerSession.Selectors
	if err := reader.New().Read(context.TODO(), session); err != nil {
		return destValue, nil, nil, err
	}

	if readerSession.Route.EnableAudit {
		r.logMetrics(readerSession.Route.URI, session.Metrics, session.Stats)
	}

	readerStats := session.Stats
	if !readerSession.IsMetricsEnabled() {
		readerStats = nil
	}

	return destValue, session.ViewMeta, readerStats, nil
}

func (r *Router) updateCache(ctx context.Context, route *Route, cacheEntry *cache.Entry, response PayloadReader) {
	if !debugEnabled {
		go r.putCache(ctx, route, cacheEntry, response)
		return
	}

	r.putCache(ctx, route, cacheEntry, response)
}

func (r *Router) cacheEntry(ctx context.Context, session *ReaderSession) (*cache.Entry, error) {
	if session.Route.Cache == nil {
		return nil, nil
	}

	cacheEntry, err := r.createCacheEntry(ctx, session)
	if err != nil {
		return nil, err
	}

	return cacheEntry, nil
}

func (r *Router) putCache(ctx context.Context, route *Route, cacheEntry *cache.Entry, payloadReader PayloadReader) {
	data, err := io.ReadAll(payloadReader)
	if err == nil {
		_ = route.Cache.Put(ctx, cacheEntry, data, payloadReader.CompressionType(), payloadReader.Headers())
	}
}

func (r *Router) runBeforeFetchIfNeeded(response http.ResponseWriter, request *http.Request, route *Route) (shouldContinue bool) {
	if route.Visitor == nil || route.Visitor._fetcher == nil {
		return true
	}

	if actual, ok := route.Visitor._fetcher.(config.BeforeFetcher); ok {
		return r.runBeforeFetch(response, request, actual.BeforeFetch)
	}

	return true
}

func (r *Router) runBeforeFetch(response http.ResponseWriter, request *http.Request, fn func(response http.ResponseWriter, request *http.Request) error) bool {
	respWrapper := httputils.NewClosableResponse(response)
	err := fn(respWrapper, request)
	if respWrapper.Closed {
		return false
	}

	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(err.Error()))
		return false
	}

	return true
}

func (r *Router) runAfterFetchIfNeeded(session *ReaderSession, dest interface{}) (shouldContinue bool) {
	if session.Route.Visitor == nil || session.Route.Visitor._fetcher == nil {
		return true
	}

	if actual, ok := session.Route.Visitor._fetcher.(config.AfterFetcher); ok {
		return r.runAfterFetch(session, dest, actual.AfterFetch)
	}

	return true
}

func (r *Router) runAfterFetch(session *ReaderSession, dest interface{}, fn func(dest interface{}, response http.ResponseWriter, req *http.Request) error) bool {
	respWrapper := httputils.NewClosableResponse(session.Response)
	err := fn(dest, session.Response, session.Request)

	if respWrapper.Closed {
		return false
	}

	if err != nil {
		r.writeErr(session.Response, session.Route, err, http.StatusBadRequest)
		return false
	}
	return true
}

func (r *Router) marshalResult(session *ReaderSession, destValue reflect.Value, viewMeta interface{}, stats []*reader.Info) (result []byte, statusCode int, err error) {
	filters, err := r.buildJsonFilters(session.Route, session.Selectors)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	format := session.RequestParams.outputQueryFormat(session.Route)

	switch strings.ToLower(format) {
	case CSVQueryFormat:
		return r.marshalAsCSV(session, destValue, filters)
	case TabularJSONQueryFormat:
		if session.Route.Style == ComprehensiveStyle {
			tabJSONInterceptors := r.tabJSONInterceptors(session, destValue, filters)
			return r.marshalAsJSON(session, destValue, filters, viewMeta, stats, tabJSONInterceptors)
		}
		return r.marshalAsTabularJSON(session, destValue, filters)
	}

	return r.marshalAsJSON(session, destValue, filters, viewMeta, stats)
}

func (r *Router) tabJSONInterceptors(session *ReaderSession, destValue reflect.Value, filters []*json.FilterEntry) json.MarshalerInterceptors {
	interceptors := make(map[string]json.MarshalInterceptor)

	f := func() ([]byte, int, error) {
		return r.marshalAsTabularJSON(session, destValue, filters)
	}

	interceptors[session.Route.Field] = json.MarshalInterceptor(f)
	return interceptors

}

func (r *Router) marshalAsJSON(session *ReaderSession, destValue reflect.Value, filters []*json.FilterEntry, viewMeta interface{}, stats []*reader.Info, options ...interface{}) ([]byte, int, error) {
	payload, httpStatus, err := r.result(session, destValue, filters, viewMeta, stats, options...)
	if err != nil {
		return nil, httpStatus, err
	}
	return payload, httpStatus, nil
}

func (r *Router) inAWS() bool {
	scheme := url.Scheme(r.resource.SourceURL, "s3")
	return scheme == "s3"
}

func (r *Router) result(session *ReaderSession, destValue reflect.Value, filters []*json.FilterEntry, meta interface{}, stats []*reader.Info, options ...interface{}) ([]byte, int, error) {
	aFilters := json.NewFilters(filters...)
	aOptions := []interface{}{aFilters}
	aOptions = append(aOptions, options...)

	if session.Route.Cardinality == view.Many {
		result := r.wrapWithResponseIfNeeded(destValue.Elem().Interface(), session.Route, meta, stats, nil)
		asBytes, err := session.Route._marshaller.Marshal(result, aOptions...)

		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil
	}

	slicePtr := unsafe.Pointer(destValue.Pointer())
	sliceSize := session.Route.View.Schema.Slice().Len(slicePtr)

	switch sliceSize {
	case 0:
		return nil, http.StatusNotFound, nil
	case 1:
		result := r.wrapWithResponseIfNeeded(session.Route.View.Schema.Slice().ValueAt(slicePtr, 0), session.Route, meta, stats, nil)
		asBytes, err := session.Route._marshaller.Marshal(result, aOptions...)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil

	default:
		return nil, http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", session.Request.RequestURI, sliceSize)
	}
}

func (r *Router) buildJsonFilters(route *Route, selectors *view.Selectors) ([]*json.FilterEntry, error) {
	entries := make([]*json.FilterEntry, 0)

	selectors.Lock()
	defer selectors.Unlock()
	for viewName, selector := range selectors.Index {
		if len(selector.Columns) == 0 {
			continue
		}

		var aPath string
		viewByName, ok := route.Index.viewByName(viewName)
		if !ok {
			aPath = ""
		} else {
			aPath = viewByName.Path
		}

		fields := make([]string, len(selector.Fields))
		for i := range selector.Fields {
			fields[i] = selector.Fields[i]
		}

		entries = append(entries, &json.FilterEntry{
			Path:   aPath,
			Fields: fields,
		})

	}

	return entries, nil
}

func (r *Router) writeErr(w http.ResponseWriter, route *Route, err error, statusCode int) {
	statusCode, message, anObjectErr := normalizeErr(err, statusCode)
	responseStatus := r.responseStatusError(message, anObjectErr)
	if route._responseSetter == nil {
		errAsBytes, marshalErr := goJson.Marshal(responseStatus)
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

	//TODO extend to unified response
	r.setResponseStatus(route, response, responseStatus, nil)

	asBytes, marErr := route._marshaller.Marshal(response.Elem().Interface())
	if marErr != nil {
		w.Write(asBytes)
		w.WriteHeader(statusCode)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(asBytes)
}

func (r *Router) responseStatusError(message string, anObject interface{}) ResponseStatus {
	responseStatus := ResponseStatus{
		Status:  "error",
		Message: message,
	}

	asEmbeddable, ok := anObject.(expand.EmbeddableMap)
	if !ok {
		responseStatus.Errors = anObject
	} else {
		responseStatus.Extras = asEmbeddable
	}

	return responseStatus
}

func (r *Router) setResponseStatus(route *Route, response reflect.Value, responseStatus ResponseStatus, stats []*reader.Info) {
	if route._responseSetter.statusField != nil {
		route._responseSetter.statusField.SetValue(unsafe.Pointer(response.Pointer()), responseStatus)
	}

	if route._responseSetter.infoField != nil {
		route._responseSetter.infoField.SetValue(unsafe.Pointer(response.Pointer()), stats)
	}
}

func (r *Router) wrapWithResponseIfNeeded(response interface{}, route *Route, viewMeta interface{}, stats []*reader.Info, state *expand.State) interface{} {
	if route._responseSetter == nil {
		return response
	}

	newResponse := reflect.New(route._responseSetter.rType)
	responseBodyPtr := unsafe.Pointer(newResponse.Pointer())
	route._responseSetter.bodyField.SetValue(responseBodyPtr, response)
	if route._responseSetter.metaField != nil && viewMeta != nil {
		route._responseSetter.metaField.SetValue(responseBodyPtr, viewMeta)
	}

	r.setResponseStatus(route, newResponse, r.responseStatusSuccess(state), stats)
	return newResponse.Elem().Interface()
}

func (r *Router) createCacheEntry(ctx context.Context, session *ReaderSession) (*cache.Entry, error) {
	session.Selectors.RWMutex.RLock()
	defer session.Selectors.RWMutex.RUnlock()

	selectorSlice := make([]*view.Selector, len(session.Selectors.Index))
	for viewName := range session.Selectors.Index {
		index, _ := session.Route.viewIndex(viewName)
		selectorSlice[index] = session.Selectors.Index[viewName]
	}
	marshalled, err := goJson.Marshal(selectorSlice)
	if err != nil {
		return nil, err
	}

	return session.Route.Cache.Get(ctx, marshalled, session.Route.View.Name)
}

func normalizeErr(err error, statusCode int) (int, string, interface{}) {
	switch actual := err.(type) {
	case *svalidator.Validation:
		var errorItems []*ErrorItem
		for _, item := range actual.Violations {
			errorItems = append(errorItems, &ErrorItem{
				Location: item.Location,
				Field:    item.Field,
				Value:    item.Value,
				Message:  item.Message,
				Check:    item.Check,
			})
		}

		return statusCode, err.Error(), errorItems
	case *govalidator.Validation:
		var items []*ErrorItem
		for _, item := range actual.Violations {
			items = append(items, &ErrorItem{
				Location: item.Location,
				Field:    item.Field,
				Value:    item.Value,
				Message:  item.Message,
				Check:    item.Check,
			})
		}

		return statusCode, actual.Error(), items
	case *JSONError:
		return statusCode, "", actual.Object
	case *Errors:
		actual.setStatus(statusCode)
		for _, anError := range actual.Errors {
			isObj := isObject(anError.Err)
			if isObj {
				statusCode, anError.Message, anError.Object = normalizeErr(anError.Err, statusCode)
			} else {
				statusCode, anError.Message, anError.Object = normalizeErr(anError.Err, statusCode)
			}
		}

		actual.setStatus(statusCode)

		return actual.status, actual.Message, actual.Errors
	case *expand.ErrorResponse:
		if actual.StatusCode != 0 {
			statusCode = actual.StatusCode
		}

		return statusCode, actual.Message, actual.Content
	default:
		return statusCode, err.Error(), nil
	}
}

func isObject(anError interface{}) bool {
	rType := reflect.TypeOf(anError)
	if rType == strErrType {
		return false
	}

	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType.Kind() == reflect.Struct
}

func (r *Router) indexRoutes() {
	for i, route := range r.routes {
		methods, _ := r.index[route.URI]
		methods = append(methods, i)
		r.index[route.URI] = methods
	}
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

func (r *Router) writeResponse(ctx context.Context, session *ReaderSession, payloadReader PayloadReader) {
	defer payloadReader.Close()

	redirected, err := r.redirectIfNeeded(ctx, session, payloadReader)
	if redirected {
		return
	}

	if err != nil {
		r.writeErr(session.Response, session.Route, err, http.StatusInternalServerError)
		return
	}

	session.Response.Header().Add(content.Type, session.RequestParams.OutputFormat+"; "+CharsetUTF8)
	session.Response.Header().Add(ContentLength, strconv.Itoa(payloadReader.Size()))
	for key, value := range payloadReader.Headers() {
		session.Response.Header().Add(key, value[0])
	}

	compressionType := payloadReader.CompressionType()
	if compressionType != "" {
		session.Response.Header().Set(content.Encoding, compressionType)
	}

	session.Response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(session.Response, payloadReader)
}

func (r *Router) redirectIfNeeded(ctx context.Context, session *ReaderSession, payloadReader PayloadReader) (redirected bool, err error) {
	redirect := r.resource.Redirect
	if redirect == nil {
		return false, nil
	}

	if redirect.MinSizeKb*1024 > payloadReader.Size() {
		return false, nil
	}

	preSign, err := redirect.Apply(ctx, session.Route.View.Name, payloadReader)
	if err != nil {
		return false, err
	}

	http.Redirect(session.Response, session.Request, preSign.URL, http.StatusMovedPermanently)
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

func (r *Router) logAudit(request *http.Request, response http.ResponseWriter, route *Route) {
	headers := request.Header.Clone()
	if authorization := headers.Get("Authorization"); authorization != "" {
		r.obfuscateAuthorization(request, response, authorization, headers, route)
	}

	for _, apiKey := range route._apiKeys {
		for key := range headers {
			if strings.EqualFold(apiKey.Header, key) {
				headers.Set(key, "*****")
			}
		}
	}

	asBytes, _ := goJson.Marshal(Audit{
		URL:     request.RequestURI,
		Headers: headers,
	})

	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) obfuscateAuthorization(request *http.Request, response http.ResponseWriter, authorization string, headers http.Header, route *Route) {
	if jwtCodec, _ := config.Config.LookupCodec(config.CodecKeyJwtClaim); jwtCodec != nil {
		if claim, _ := jwtCodec.Valuer().Value(context.TODO(), authorization); claim != nil {
			if jwtClaim, ok := claim.(*jwt.Claims); ok && jwtClaim != nil {
				headers.Set("User-ID", strconv.Itoa(jwtClaim.UserID))
				headers.Set("User-Email", jwtClaim.Email)
				if route.IsMetricsEnabled(request) {
					response.Header().Set("User-ID", strconv.Itoa(jwtClaim.UserID))
					response.Header().Set("User-Email", jwtClaim.Email)
				}
			}
		}
	}
	headers.Set("Authorization", "***")
}

func (r *Router) logMetrics(URI string, metrics []*reader.Metric, stats []*reader.Info) {
	asBytes, _ := goJson.Marshal(struct {
		URI     string
		Metrics []*reader.Metric
		Stats   []*reader.Info
	}{URI: URI, Metrics: metrics, Stats: stats})

	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) initMatcher() {
	r.Matcher = NewRouteMatcher(r.routes)
}

func (r *Router) normalizeURI(prefix string, URI string) string {
	if strings.HasPrefix(URI, prefix) {
		return URI
	}

	if r.resource.URL != "" {
		return url.Join(prefix, r.resource.URL, URI)
	}

	return url.Join(prefix, URI)
}

func (r *Router) marshalAsCSV(session *ReaderSession, sliceValue reflect.Value, filters []*json.FilterEntry) ([]byte, int, error) {
	if session.Route.View.Schema.Slice().Len(unsafe.Pointer(sliceValue.Pointer())) == 0 {
		return nil, http.StatusOK, nil
	}

	fieldsLen := 0
	for _, filter := range filters {
		fieldsLen += len(filter.Fields)
	}

	fields := make([]string, 0, fieldsLen)
	offset := 0
	for _, filter := range filters {
		updateFieldPathsIfNeeded(filter)
		offset = copy(fields[offset:], filter.Fields)
	}

	data, err := session.Route.CSV._outputMarshaller.Marshal(sliceValue.Elem().Interface())

	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return data, http.StatusOK, nil
}

func (r *Router) marshalAsTabularJSON(session *ReaderSession, sliceValue reflect.Value, filters []*json.FilterEntry) ([]byte, int, error) {
	if session.Route.View.Schema.Slice().Len(unsafe.Pointer(sliceValue.Pointer())) == 0 {
		return nil, http.StatusOK, nil
	}

	fieldsLen := 0
	for _, filter := range filters {
		fieldsLen += len(filter.Fields)
	}

	fields := make([]string, 0, fieldsLen)
	offset := 0
	for _, filter := range filters {
		updateFieldPathsIfNeeded(filter)
		offset = copy(fields[offset:], filter.Fields)
	}

	data, err := session.Route.TabularJSON._outputMarshaller.Marshal(sliceValue.Elem().Interface())

	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return data, http.StatusOK, nil
}

func (r *Router) responseStatusSuccess(state *expand.State) ResponseStatus {
	status := ResponseStatus{Status: "ok"}
	if state != nil {
		status.Extras = state.ResponseBuilder.Content
	}

	return status
}

func (r *Router) Interceptor() (*RouteInterceptor, bool) {
	return r.resource.Interceptor, r.resource.Interceptor != nil
}

func (r *Router) Resource() *Resource {
	return r.resource
}

func (r *Router) payloadReader(ctx context.Context, session *ReaderSession, route *Route) (PayloadReader, error) {
	if route.Async != nil {
		return r.readAsyncResponse(ctx, session)
	}

	return r.readSyncResponse(ctx, session)
}

func (r *Router) readSyncResponse(ctx context.Context, session *ReaderSession) (PayloadReader, error) {
	cacheEntry, err := r.cacheEntry(ctx, session)
	if err != nil {
		r.writeErr(session.Response, session.Route, err, http.StatusInternalServerError)
	}

	if cacheEntry != nil && cacheEntry.Has() {
		return cacheEntry, nil
	}

	response, err := r.readResponse(ctx, session)
	if err != nil {
		return nil, err
	}

	if cacheEntry != nil {
		r.updateCache(ctx, session.Route, cacheEntry, response)
	}

	return response, err
}

func (r *Router) readAsyncResponse(ctx context.Context, session *ReaderSession) (PayloadReader, error) {
	async := session.Route.Async
	var qualifierValue *string

	if asyncQualifier := async._qualifier; asyncQualifier != nil && asyncQualifier.parameter != nil {
		value, err := session.RequestParams.ExtractHttpParam(ctx, asyncQualifier.parameter)
		if err != nil {
			return nil, err
		}

		if asyncQualifier.accessor != nil {
			value, err = asyncQualifier.accessor.Value(value)
			if err != nil {
				return nil, err
			}
		}

		marshal, err := goJson.Marshal(value)
		if err != nil {
			return nil, err
		}

		asString := string(marshal)
		qualifierValue = &asString
	}

	record := &AsyncRecord{
		CreationTime: time.Now(),
		JobID:        uuid.New().String(),
		Qualifier:    qualifierValue,
		State:        AsyncStateRunning,
	}

	if _, _, err := async._inserter.Exec(ctx, record); err != nil {
		return nil, err
	}

	payloadReader, err := r.marshalAsyncRecord(session, record)
	if err != nil {
		return nil, err
	}

	go r.readAsync(ctx, session, record)

	return payloadReader, nil
}

func (r *Router) marshalAsyncRecord(session *ReaderSession, record *AsyncRecord) (PayloadReader, error) {
	marshal, err := session.Route.JSON._marshaller.Marshal(record)
	if err != nil {
		return nil, err
	}

	payloadReader, err := r.compressIfNeeded(marshal, session.Route)
	if err != nil {
		return nil, err
	}

	return payloadReader, nil
}

func (r *Router) readAsync(ctx context.Context, session *ReaderSession, record *AsyncRecord) {
	response, err := r.readSyncResponse(ctx, session)
	if err != nil {
		_, message, object := normalizeErr(err, 400)
		if object != nil {
			marshal, _ := session.Route.JSON._marshaller.Marshal(object)
			asString := string(marshal)
			record.Error = &asString
		} else {
			record.Error = &message
		}
	} else {
		data, err := io.ReadAll(response)
		if err != nil {
			message := err.Error()
			record.Error = &message
		} else {
			result := string(data)
			record.Value = &result
		}
	}

	record.State = AsyncStateDone
	async := session.Route.Async
	if _, err = async._updater.Exec(ctx, record); err != nil {
		fmt.Printf("[ERROR] error when trying to update async record: %v\n", err.Error())
	}
}

func updateFieldPathsIfNeeded(filter *json.FilterEntry) {
	if filter.Path == "" {
		return
	}

	for i, field := range filter.Fields {
		filter.Fields[i] = filter.Path + "." + field
	}
}

func ExtractCacheableViews(routes ...*Route) []*view.View {
	var views []*view.View

	for _, route := range routes {
		appendCacheWarmupViews(route.View, &views)
	}

	return views
}

func appendCacheWarmupViews(aView *view.View, result *[]*view.View) {
	if aCache := aView.Cache; aCache != nil && aCache.Warmup != nil {
		*result = append(*result, aView)
	}
	if len(aView.With) == 0 {
		return
	}
	for i := range aView.With {
		appendCacheWarmupViews(&aView.With[i].Of.View, result)
	}
}
