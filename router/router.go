package router

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	goJson "encoding/json"
	"fmt"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/config"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/async/handler"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/debug"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/govalidator"
	svalidator "github.com/viant/sqlx/io/validator"
	async2 "github.com/viant/xdatly/handler/async"
	haHttp "github.com/viant/xdatly/handler/http"
	"github.com/viant/xunsafe"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// TODO: Add to meta response size
type viewHandler func(response http.ResponseWriter, request *http.Request, record *async2.Job)

const (
	Separator = ", "
)

type (
	Router struct {
		Matcher   *matcher.Matcher
		_mux      sync.Mutex
		_resource *Resource
		_index    map[string][]int
		_routes   Routes
		_queue    []func()
	}

	BytesReadCloser struct {
		bytes *bytes.Buffer
	}

	MatchableRoute struct {
		Route *Route
	}

	ApiPrefix string

	ReaderSession struct {
		RequestParams *RequestParams
		Route         *Route
		Request       *http.Request
		Response      http.ResponseWriter
		Selectors     *view.States
	}

	preparedResponse struct {
		objects  interface{}
		viewMeta interface{}
		stats    []*reader.Info
		session  *reader.Session
		result   interface{}
	}
)

func (m *MatchableRoute) URI() string {
	return m.Route.URI
}

func (m *MatchableRoute) Namespaces() []string {
	methods := []string{m.Route.Method}
	if m.Route.CorsEnabled() {
		methods = append(methods, http.MethodOptions)
	}

	return methods
}

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
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyInfoHeaderValue
}

func (r *Route) IsMetricDebug(req *http.Request) bool {
	if !r.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyDebugHeaderValue
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

	return (*s.Route.EnableDebug) && (s.Request.Header.Get(httputils.DatlyRequestDisableCacheHeader) != "" || s.Request.Header.Get(strings.ToLower(httputils.DatlyRequestDisableCacheHeader)) != "")
}

func (b *BytesReadCloser) Read(p []byte) (int, error) {
	return b.bytes.Read(p)
}

func (b *BytesReadCloser) Close() error {
	return nil
}

func (r *Router) View(name string) (*view.View, error) {
	return r._resource.Resource.View(name)
}

func (r *Router) Handle(response http.ResponseWriter, request *http.Request) error {
	return r.HandleAsync(response, request, nil)
}

func (r *Router) HandleAsync(response http.ResponseWriter, request *http.Request, record *async2.Job) error {
	if r._resource.Interceptor != nil {
		_, err := r._resource.Interceptor.Intercept(request)
		if err != nil {
			code, message := httputils.BuildErrorResponse(err)
			response.WriteHeader(code)
			response.Write([]byte(message))
			return nil
		}
	}

	route, err := r.Matcher.MatchOne(request.Method, request.URL.Path)
	if err != nil {
		return err
	}

	return r.HandleAsyncRoute(response, request, route.(*MatchableRoute).Route, record)
}

func (r *Router) HandleRoute(response http.ResponseWriter, request *http.Request, route *Route) error {
	return r.HandleAsyncRoute(response, request, route, nil)
}

func (r *Router) HandleAsyncRoute(response http.ResponseWriter, request *http.Request, route *Route, record *async2.Job) error {
	err := r.AuthorizeRequest(request, route)
	if err != nil {
		httputils.WriteError(response, err)
		return nil
	}

	if request.Method == http.MethodOptions {
		corsHandler(request, route.Cors)(response)
		return nil
	}

	r.viewHandler(route)(response, request, record)
	return nil
}

func (r *Router) AuthorizeRequest(request *http.Request, route *Route) error {
	apiKey := route.APIKey
	if apiKey == nil {
		return nil
	}

	key := request.Header.Get(apiKey.Header)
	if key != apiKey.Value {
		return httputils.NewHttpMessageError(http.StatusUnauthorized, nil)
	}

	return nil
}

func (r *Router) prepareViewHandler(response http.ResponseWriter, request *http.Request, route *Route) {
	if route.Cors != nil {
		enableCors(response, request, route.Cors, false)
	}

	if route.EnableAudit {
		r.logAudit(request, response, route)
	}
}

func New(resource *Resource, options ...interface{}) (*Router, error) {
	var apiPrefix string
	for _, option := range options {
		switch actual := option.(type) {
		case ApiPrefix:
			apiPrefix = string(actual)
		}
	}

	router := &Router{
		_resource: resource,
		_index:    map[string][]int{},
		_routes:   resource.Routes,
	}

	return router, router.Init(resource.Routes, apiPrefix)
}

func (r *Router) Init(routes Routes, apiPrefix string) error {
	for _, route := range routes {
		route.URI = r.normalizeURI(apiPrefix, route.URI)

		route._resource = r._resource.Resource
		route._router = r
	}

	r.indexRoutes()
	r.initMatcher()

	if r._resource.URL != "" {
		r._resource.URL = r.normalizeURI(apiPrefix, r._resource.URL)
	}

	if r._resource.Interceptor != nil {
		if err := r._resource.Interceptor.init(r._resource.URL); err != nil {
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
		writer.Header().Set(httputils.AllowOriginHeader, "*")
	} else {
		writer.Header().Set(httputils.AllowOriginHeader, origin)
	}

	if cors.AllowMethods != nil && allHeaders {
		writer.Header().Set(httputils.AllowMethodsHeader, request.Method)
	}

	if cors.AllowHeaders != nil && allHeaders {
		writer.Header().Set(httputils.AllowHeadersHeader, strings.Join(*cors.AllowHeaders, Separator))
	}
	if cors.AllowCredentials != nil && allHeaders {
		writer.Header().Set(httputils.AllowCredentialsHeader, strconv.FormatBool(*cors.AllowCredentials))
	}
	if cors.MaxAge != nil && allHeaders {
		writer.Header().Set(httputils.MaxAgeHeader, strconv.Itoa(int(*cors.MaxAge)))
	}

	if cors.ExposeHeaders != nil && allHeaders {
		writer.Header().Set(httputils.ExposeHeadersHeader, strings.Join(*cors.ExposeHeaders, Separator))
	}
}

func (r *Router) Serve(serverPath string) error {
	return http.ListenAndServe(serverPath, r)
}

func (r *Router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	route, err := r.Matcher.MatchOne(request.Method, request.URL.Path)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = r.HandleRoute(writer, request, route.(*MatchableRoute).Route)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request, record *async2.Job) {
		if !r.runBeforeFetchIfNeeded(response, request, route) {
			return
		}

		ctx := context.Background()
		payloadReader, err := r.payloadReader(ctx, request, response, route, record)
		if err != nil {
			code, _ := httputils.BuildErrorResponse(err)
			r.writeErr(response, route, err, code)
			return
		}

		if payloadReader != nil {
			r.writeResponse(ctx, request, response, route, payloadReader)
		}
	}
}

func (r *Router) prepareReaderSession(ctx context.Context, response http.ResponseWriter, request *http.Request, route *Route) (*ReaderSession, error) {
	requestParams, err := NewRequestParameters(request, route)
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, err)
	}

	if route.CSV == nil && requestParams.OutputContentType == CSVContentType {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output CSV config?)", CSVContentType)))
	}
	if route.TabularJSON == nil && route.DataFormat == JSONDataFormatTabular {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output DataFormat config?)", JSONContentType)))
	}

	if route.XML == nil && route.DataFormat == XMLFormat {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output DataFormat config?)", XMLContentType)))
	}

	selectors, _, err := CreateSelectorsFromRoute(ctx, route, request, requestParams, route.Index._viewDetails...)
	if err != nil {
		defaultCode := http.StatusBadRequest
		if route.ParamStatusError != nil {
			defaultCode = *route.ParamStatusError
		}
		return nil, httputils.ErrorOf(defaultCode, err)
	}

	return &ReaderSession{
		RequestParams: requestParams,
		Route:         route,
		Request:       request,
		Response:      response,
		Selectors:     selectors,
	}, nil
}

func UnsupportedFormatErr(format string) error {
	return fmt.Errorf("unsupported output format %v", format)
}

func (r *Router) readResponse(ctx context.Context, session *ReaderSession) (PayloadReader, error) {
	response, ok, err := r.prepareResponse(session, session.IsMetricDebug(), session.IsMetricsEnabled())
	if !ok || err != nil {
		return nil, err
	}

	resultMarshalled, err := r.marshalResult(session, response)
	if err != nil {
		return nil, err
	}

	payloadReader, err := r.compressIfNeeded(resultMarshalled, session.Route)
	if err != nil {
		return nil, err
	}

	templateMeta := session.Route.View.Template.Meta
	if templateMeta != nil && templateMeta.Kind == view.MetaTypeHeader && response.viewMeta != nil {
		data, err := goJson.Marshal(response.viewMeta)
		if err != nil {
			return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
		}

		payloadReader.AddHeader(templateMeta.Name, string(data))
	}

	for _, stat := range response.stats {
		marshal, err := goJson.Marshal(stat)
		if err != nil {
			continue
		}

		payloadReader.AddHeader(httputils.DatlyResponseHeaderMetrics+"-"+stat.Name(), string(marshal))
	}

	return payloadReader, nil
}

func (r *Router) prepareResponse(session *ReaderSession, includeSQL bool, metricEnabled bool) (*preparedResponse, bool, error) {
	readerSession, err := r.readValue(session, includeSQL, metricEnabled)
	if err != nil {
		return nil, false, err
	}

	if !r.runAfterFetchIfNeeded(session, readerSession.Dest) {
		return nil, false, nil
	}

	viewMeta := readerSession.ViewMeta
	readerStats := readerSession.Stats
	value := reflect.ValueOf(readerSession.Dest).Elem().Interface()
	result, err := r.result(session, value, viewMeta, readerStats)
	if err != nil {
		return nil, false, err
	}

	return &preparedResponse{
		result:   result,
		objects:  value,
		viewMeta: viewMeta,
		stats:    readerStats,
		session:  readerSession,
	}, true, nil
}

func (r *Router) readValue(readerSession *ReaderSession, includeSQL bool, metricEnabled bool) (*reader.Session, error) {
	destValue := reflect.New(readerSession.Route.View.Schema.SliceType())
	dest := destValue.Interface()

	session, err := reader.NewSession(dest, readerSession.Route.View)
	if err != nil {
		return nil, err
	}
	session.CacheDisabled = readerSession.IsCacheDisabled()
	session.IncludeSQL = includeSQL
	session.States = readerSession.Selectors
	if err := reader.New().Read(context.TODO(), session); err != nil {
		return nil, err
	}

	if readerSession.Route.EnableAudit {
		r.logMetrics(readerSession.Route.URI, session.Metrics, session.Stats)
	}

	if !metricEnabled {
		session.Stats = nil
	}

	return session, nil
}

func (r *Router) updateCache(ctx context.Context, route *Route, cacheEntry *cache.Entry, response PayloadReader) {
	if !debug.Enabled {
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

func (r *Router) marshalResult(session *ReaderSession, response *preparedResponse) (result []byte, err error) {
	filters, err := r.buildJsonFilters(session.Route, session.Selectors)
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, err)
	}

	format := session.RequestParams.dataFormat(session.Route)

	switch strings.ToLower(format) {
	case XLSFormat:
		return r.marshalAsXLS(session, response, filters)
	case CSVFormat:
		return r.marshalAsCSV(session, response, filters)
	case XMLFormat:
		return r.marshalAsXML(session, response.objects, filters)
	case JSONDataFormatTabular:
		if session.Route.Style == ComprehensiveStyle {
			tabJSONInterceptors := r.tabJSONInterceptors(session, response.objects, filters)
			return r.marshalAsJSON(session, response, filters, tabJSONInterceptors)
		}
		return r.marshalAsTabularJSON(session, response.objects, filters)
	case JSONFormat:
		return r.marshalAsJSON(session, response, json.NewFilters(filters...))
	default:
		return nil, fmt.Errorf("unsupproted data format: %s", format)
	}

}

func (r *Router) tabJSONInterceptors(session *ReaderSession, destValue interface{}, filters []*json.FilterEntry) json.MarshalerInterceptors {
	interceptors := make(map[string]json.MarshalInterceptor)

	f := func() ([]byte, error) {
		return r.marshalAsTabularJSON(session, destValue, filters)
	}

	interceptors[session.Route.Field] = json.MarshalInterceptor(f)
	return interceptors

}

func (r *Router) marshalAsJSON(session *ReaderSession, response *preparedResponse, options ...interface{}) ([]byte, error) {
	marshal, err := session.Route._jsonMarshaller.Marshal(response.result, options...)

	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return marshal, nil
}

func (r *Router) inAWS() bool {
	scheme := url.Scheme(r._resource.SourceURL, "s3")
	return scheme == "s3"
}

func (r *Router) result(session *ReaderSession, destValue interface{}, meta interface{}, stats []*reader.Info) (interface{}, error) {
	if session.Route.Cardinality == state.Many {
		result := r.wrapWithResponseIfNeeded(destValue, session.Route, meta, stats, nil)
		return result, nil
	}

	slicePtr := xunsafe.AsPointer(destValue)
	sliceSize := session.Route.View.Schema.Slice().Len(slicePtr)

	switch sliceSize {
	case 0:
		return nil, httputils.NewHttpMessageError(http.StatusNotFound, nil)
	case 1:
		result := r.wrapWithResponseIfNeeded(session.Route.View.Schema.Slice().ValueAt(slicePtr, 0), session.Route, meta, stats, nil)
		return result, nil
	default:
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", session.Request.RequestURI, sliceSize))
	}
}

func (r *Router) buildJsonFilters(route *Route, selectors *view.States) ([]*json.FilterEntry, error) {
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
	if statusCode < http.StatusBadRequest {
		statusCode = http.StatusBadRequest
	}

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

	asBytes, marErr := route._jsonMarshaller.Marshal(response.Elem().Interface())
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

	selectorSlice := make([]*view.State, len(session.Selectors.Index))
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
	case *httputils.Errors:
		actual.SetStatus(statusCode)
		for _, anError := range actual.Errors {
			isObj := types.IsObject(anError.Err)
			if isObj {
				statusCode, anError.Message, anError.Object = normalizeErr(anError.Err, statusCode)
			} else {
				statusCode, anError.Message, anError.Object = normalizeErr(anError.Err, statusCode)
			}
		}

		actual.SetStatus(statusCode)

		return actual.ErrorStatusCode(), actual.Message, actual.Errors
	case *expand.ErrorResponse:
		if actual.StatusCode != 0 {
			statusCode = actual.StatusCode
		}

		return statusCode, actual.Message, actual.Content
	default:
		return statusCode, err.Error(), nil
	}
}

func (r *Router) indexRoutes() {
	for i, route := range r._routes {
		methods, _ := r._index[route.URI]
		methods = append(methods, i)
		r._index[route.URI] = methods
	}
}

func (r *Router) Routes(route string) []*Route {
	if route == "" {
		return r._routes
	}

	uriRoutes, ok := r._index[route]
	if !ok {
		return []*Route{}
	}

	routes := make([]*Route, len(uriRoutes))
	for i, routeIndex := range uriRoutes {
		routes[i] = r._routes[routeIndex]
	}

	return routes
}

func (r *Router) writeResponse(ctx context.Context, request *http.Request, response http.ResponseWriter, route *Route, payloadReader PayloadReader) {
	defer payloadReader.Close()

	redirected, err := r.redirectIfNeeded(ctx, request, response, route, payloadReader)
	if redirected {
		return
	}

	if err != nil {
		r.writeErr(response, route, err, http.StatusInternalServerError)
		return
	}

	response.Header().Add(httputils.ContentLength, strconv.Itoa(payloadReader.Size()))
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

func (r *Router) redirectIfNeeded(ctx context.Context, request *http.Request, response http.ResponseWriter, route *Route, payloadReader PayloadReader) (redirected bool, err error) {
	redirect := r._resource.Redirect
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

	buffer, err := httputils.Compress(bytes.NewReader(marshalled))
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	payloadSize := buffer.Len()
	if r.inAWS() {
		payloadSize = base64.StdEncoding.EncodedLen(payloadSize)
	}

	return AsBytesReader(buffer, httputils.EncodingGzip, payloadSize), nil
}

func (r *Router) logAudit(request *http.Request, response http.ResponseWriter, route *Route) {
	headers := request.Header.Clone()
	Sanitize(request, route, headers, response)

	asBytes, _ := goJson.Marshal(Audit{
		URL:     request.RequestURI,
		Headers: headers,
	})
	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) logMetrics(URI string, metrics []*reader.Metric, stats []*reader.Info) {
	asBytes, _ := goJson.Marshal(NewMetrics(URI, metrics, stats))

	fmt.Printf("%v\n", string(asBytes))
}

func (r *Router) initMatcher() {
	r.Matcher = matcher.NewMatcher(asMatchables(r._routes))
}

func asMatchables(routes Routes) []matcher.Matchable {
	result := make([]matcher.Matchable, 0, len(routes))
	for _, route := range routes {
		result = append(result, &MatchableRoute{Route: route})
	}
	return result
}

func (r *Router) normalizeURI(prefix string, URI string) string {
	if strings.HasPrefix(URI, prefix) {
		return URI
	}

	if r._resource.URL != "" {
		return url.Join(prefix, r._resource.URL, URI)
	}

	return url.Join(prefix, URI)
}

func (r *Router) marshalAsCSV(session *ReaderSession, response *preparedResponse, filters []*json.FilterEntry) ([]byte, error) {
	if session.Route.View.Schema.Slice().Len(xunsafe.AsPointer(response.objects)) == 0 {
		return nil, nil
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

	data, err := session.Route.CSV._outputMarshaller.Marshal(response.objects)

	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return data, nil
}

func (r *Router) marshalAsTabularJSON(session *ReaderSession, items interface{}, filters []*json.FilterEntry) ([]byte, error) {
	if session.Route.View.Schema.Slice().Len(xunsafe.AsPointer(items)) == 0 {
		return nil, nil
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

	data, err := session.Route.TabularJSON._outputMarshaller.Marshal(items)

	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return data, nil
}

func (r *Router) marshalAsXML(session *ReaderSession, items interface{}, filters []*json.FilterEntry) ([]byte, error) {
	if session.Route.View.Schema.Slice().Len(xunsafe.AsPointer(items)) == 0 {
		return nil, nil
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

	data, err := session.Route.XML._outputMarshaller.Marshal(items)

	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return data, nil
}

func (r *Router) responseStatusSuccess(state *expand.State) ResponseStatus {
	status := ResponseStatus{Status: "ok"}
	if state != nil {
		status.Extras = state.ResponseBuilder.Content
	}

	return status
}

func (r *Router) Interceptor() (*RouteInterceptor, bool) {
	return r._resource.Interceptor, r._resource.Interceptor != nil
}

func (r *Router) Resource() *Resource {
	return r._resource
}

func (r *Router) payloadReader(ctx context.Context, request *http.Request, response http.ResponseWriter, route *Route, record *async2.Job) (PayloadReader, error) {
	switch route.Service {
	case ServiceTypeExecutor:
		return r.executorPayloadReader(ctx, response, request, route)
	case ServiceTypeReader:
		session, err := r.prepareReaderSession(ctx, response, request, route)
		if err != nil {
			return nil, err
		}
		payloadReader, err := r.readerPayloadReader(ctx, route, session, record)
		if payloadReader != nil && payloadReader.Headers().Get(content.Type) == "" {
			payloadReader.Headers().Add(content.Type, session.RequestParams.OutputContentType+"; "+httputils.CharsetUTF8)
		}
		//TODO Add support for Content-Disposition: attachment; filename="document.doc"

		return payloadReader, err
	}

	return nil, httputils.NewHttpMessageError(500, fmt.Errorf("unsupported ServiceType %v", route.Service))
}

func (r *Router) marshalCustomOutput(output interface{}, route *Route) (PayloadReader, error) {
	switch actual := output.(type) {
	case haHttp.Response:
		responseContent, err := r.extractValueFromResponse(route, actual)
		if err != nil {
			return nil, err
		}

		return NewBytesReader(responseContent, "", WithHeaders(actual.Headers())), nil

	case []byte:
		return NewBytesReader(actual, ""), nil
	default:
		marshal, err := route._jsonMarshaller.Marshal(output)
		if err != nil {
			return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
		}

		return NewBytesReader(marshal, "", WithHeader(HeaderContentType, applicationJson)), nil
	}
}

func (r *Router) extractValueFromResponse(route *Route, actual haHttp.Response) ([]byte, error) {
	value := actual.Value()
	switch responseValue := value.(type) {
	case []byte:
		return responseValue, nil
	default:
		return route._jsonMarshaller.Marshal(route, responseValue)
	}
}

func (r *Router) readerPayloadReader(ctx context.Context, route *Route, session *ReaderSession, record *async2.Job) (PayloadReader, error) {
	if route.Async != nil {
		return r.readAsyncResponse(ctx, session, record)
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

func (r *Router) readAsyncResponse(ctx context.Context, session *ReaderSession, record *async2.Job) (PayloadReader, error) {
	if record != nil {
		err := r.executeAsync(context.Background(), session, record, true)
		if err != nil {
			return nil, err
		}

		return NewBytesReader(nil, ""), nil
	}

	record, err := NewAsyncRecord(ctx, session.Route, session.RequestParams)
	if err != nil {
		return nil, err
	}

	connector, err := r._resource.Resource.Connector(record.DestinationConnector)
	if err != nil {
		return nil, err
	}
	_, err = session.Route._async.EnsureTable(ctx, connector, &async.TableConfig{
		RecordType:     session.Route.View.Schema.Type(),
		TableName:      session.Route.View.Async.Table,
		Dataset:        record.DestinationDataset,
		CreateIfNeeded: true,
		GenerateAutoPk: true,
	})

	if err != nil {
		return nil, err
	}

	DB, err := connector.DB()
	if err != nil {
		return nil, err
	}

	if _, err = r.insertAndExecuteJob(ctx, session, DB, record, session.Route.Async._asyncHandler, nil); err != nil {
		return nil, err
	}

	payloadReader, err := r.marshalAsyncRecord(session, record)
	if err != nil {
		return nil, err
	}

	return payloadReader, nil
}

func (r *Router) insertAndExecuteJob(ctx context.Context, session *ReaderSession, db *sql.DB, record *async2.Job, handler async.Handler, exist *async2.OnExist) (existingJob *async2.Job, err error) {
	inserter, err := session.Route.JobsInserter(ctx, db)
	if err != nil {
		return nil, err
	}

	if _, _, err := inserter.Exec(ctx, record); err != nil {
		if exist == nil {
			return nil, err
		}

		if exist.Return {
			foundJob, selectErr := async.QueryJobByID(ctx, db, record.JobID)
			if selectErr != nil {
				return nil, err
			}

			if !(foundJob.State == async2.StateDone && foundJob.EndTime != nil && time.Now().After(*foundJob.EndTime)) { //Job doesn't expire yet
				return foundJob, nil
			}

			affected, refreshErr := async.RefreshJobByID(ctx, db, record.JobID)
			if err != nil || affected == 0 { //some other call is already calling async job
				return foundJob, refreshErr
			}

			*record = *foundJob
		}
	}

	httpRecord, err := NewAsyncHTTPRecord(session, record)
	if err != nil {
		return existingJob, err
	}

	if handler != nil {
		return existingJob, handler.Handle(ctx, httpRecord, session.Request)
	}

	r.readAsync(context.Background(), session, copyJob(record))
	return existingJob, nil
}

func copyJob(record *async2.Job) *async2.Job {
	newJob := *record
	return &newJob
}

func (r *Router) marshalAsyncRecord(session *ReaderSession, record *async2.Job) (PayloadReader, error) {
	marshal, err := session.Route.JSON._jsonMarshaller.Marshal(record)
	if err != nil {
		return nil, err
	}

	payloadReader, err := r.compressIfNeeded(marshal, session.Route)
	if err != nil {
		return nil, err
	}

	return payloadReader, nil
}

func (r *Router) readAsync(ctx context.Context, session *ReaderSession, record *async2.Job) {
	go func() {
		err := r.executeAsync(ctx, session, record, true)
		if err != nil {
			connector, connErr := r.asyncConnector(session.Route, record)
			if connErr == nil {
				db, dbErr := connector.DB()
				if dbErr == nil {
					r.handleReadAsyncErrorWithDb(ctx, session, record, db, err)
				} else {
					fmt.Printf("[ERROR] coulnd't update Job %v with status error due to %v\n", record.JobID, dbErr)
				}
			}
			fmt.Printf("[ERROR] %v\n", err.Error())
		}
	}()
}

func (r *Router) executeAsync(ctx context.Context, session *ReaderSession, record *async2.Job, forceInMemory bool) error {
	connector, err := r.asyncConnector(session.Route, record)
	if err != nil {
		return err
	}

	db, err := connector.DB()
	if err != nil {
		return err
	}

	if aHandler := session.Route.Async._asyncHandler; aHandler != nil && !forceInMemory {
		asyncHttp, err := NewAsyncHTTPRecord(session, record)
		if err != nil {
			r.handleReadAsyncErrorWithDb(ctx, session, record, db, err)
			return nil
		}

		return aHandler.Handle(ctx, asyncHttp, session.Request)
	}

	if err := r.populateAsyncRecord(ctx, session, record); err != nil {
		fmt.Printf("[ERROR] error occurred when executing async view: %v\n", err.Error())
		r.handleReadAsyncErrorWithDb(ctx, session, record, db, err)
		return nil
	}

	return nil
}

func NewAsyncHTTPRecord(session *ReaderSession, record *async2.Job) (*handler.RecordWithHttp, error) {
	bodyContent, err := session.RequestParams.BodyContent()
	if err != nil {
		return nil, err
	}

	var body string
	if len(bodyContent) > 0 {
		body = string(bodyContent)
	}

	return &handler.RecordWithHttp{
		Record:  record,
		Body:    body,
		Method:  session.Request.Method,
		URL:     session.Request.URL.String(),
		Headers: session.Request.Header,
	}, nil
}

func (r *Router) populateAsyncRecord(ctx context.Context, session *ReaderSession, record *async2.Job) error {
	record.State = async2.StateDone
	now := time.Now()
	response, _, err := r.prepareResponse(session, true, true)
	elapsed := time.Now().Sub(now)
	record.TimeTaken = &elapsed

	if err != nil {
		return err
	}

	metrics := NewMetrics(session.Request.RequestURI, response.session.Metrics, response.stats)
	asBytes, _ := goJson.Marshal(metrics)
	record.Metrics = string(asBytes)
	connector, err := r.asyncConnector(session.Route, record)
	if err != nil {
		return err
	}

	for _, stat := range response.stats {
		for _, templateStat := range stat.Template {
			if templateStat.SQL != "" {
				record.SQL = append(record.SQL, &async2.SQL{Query: templateStat.SQL, Args: templateStat.Args})
			}
		}
	}

	if err != nil {
		return err
	}

	err = r.prepareAndPutAsyncRecords(ctx, session, response, record, connector)
	return err
}

func (r *Router) updateAsyncRecord(ctx context.Context, session *ReaderSession, record *async2.Job, db *sql.DB) error {
	updater, err := session.Route.Async.JobsUpdater(ctx, db)
	if err != nil {
		return fmt.Errorf("error when connecting with Async connector: %v\n", err.Error())
	}

	if _, err = updater.Exec(ctx, record); err != nil {
		return fmt.Errorf("error when trying to update async record: %v\n", err.Error())
	}

	return nil
}

func (r *Router) handleReadAsyncErrorWithDb(ctx context.Context, session *ReaderSession, record *async2.Job, db *sql.DB, err error) {
	record.State = async2.StateDone
	_, message, object := normalizeErr(err, 400)
	if object != nil {
		marshal, _ := session.Route.JSON._jsonMarshaller.Marshal(object)
		asString := string(marshal)
		record.Error = &asString
	} else {
		record.Error = &message
	}

	err = r.updateAsyncRecord(ctx, session, record, db)
	if err != nil {
		fmt.Printf("[ERROR] %v\n", err.Error())
	}
}

func (r *Router) prepareAndPutAsyncRecords(ctx context.Context, session *ReaderSession, response *preparedResponse, record *async2.Job, connector *view.Connector) error {
	if _, err := session.Route._async.EnsureTable(ctx, connector, &async.TableConfig{
		RecordType:     types.EnsureStruct(reflect.TypeOf(response.objects)),
		TableName:      record.DestinationTable,
		Dataset:        record.DestinationDataset,
		CreateIfNeeded: record.DestinationCreateDisposition == async2.CreateDispositionIfNeeded,
		GenerateAutoPk: true,
	}); err != nil {
		return err
	}

	db, err := session.Route.Async.Connector.DB()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	err = r.putAsyncRecord(ctx, session, response, record, db, tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (r *Router) putAsyncRecord(ctx context.Context, session *ReaderSession, response *preparedResponse, record *async2.Job, db *sql.DB, tx *sql.Tx) error {
	recordsInserter, err := session.Route.RecordsInserter(ctx, session.Route, db)
	if err != nil {
		return err
	}

	_, _, err = recordsInserter.Exec(ctx, response.objects, tx)
	if err != nil {
		return err
	}

	updater, err := session.Route.Async.JobsUpdater(ctx, db)
	if err != nil {
		return err
	}

	_, err = updater.Exec(ctx, record, tx)
	return err
}

func (r *Router) QueryAllJobs(writer http.ResponseWriter, request *http.Request) {
	ctx := context.Background()
	jobs, err := r.FindAllJobs(ctx, request)
	if err != nil {
		r.normalizeAndWriteErr(writer, err)
		return
	}

	marshal, err := goJson.Marshal(jobs)
	if err != nil {
		r.normalizeAndWriteErr(writer, err)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(marshal)
}

func (r *Router) normalizeAndWriteErr(writer http.ResponseWriter, err error) {
	statusCode, message, errorObject := normalizeErr(err, 400)
	if errorObject != nil {
		marshal, err := goJson.Marshal(errorObject)
		if err == nil {
			message = string(marshal)
		}
	}

	writer.WriteHeader(statusCode)
	_, _ = writer.Write([]byte(message))
}

func (r *Router) FindAllJobs(ctx context.Context, request *http.Request) ([]*async2.Job, error) {
	jobs, err := r.PrepareJobs(ctx, request)
	if err != nil {
		return nil, err
	}

	var allRecords []*async2.Job
	for db, qualifiers := range jobs {
		records, err := async.QueryJobs(ctx, db, qualifiers...)
		if err != nil {
			return nil, err
		}

		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

func (r *Router) PrepareJobs(ctx context.Context, request *http.Request) (map[*sql.DB][]*async.JobQualifier, error) {
	jobs := async.NewJobs()

	for _, route := range r._routes {
		rAsync := route.Async
		if rAsync == nil {
			continue
		}

		if err := r.AuthorizeRequest(request, route); err != nil {
			continue
		}

		db, err := rAsync.Connector.DB()
		if err != nil {
			return nil, err
		}

		parameters, err := NewRequestParameters(request, route)
		if err != nil {
			return nil, err
		}

		subject, err := PrincipalSubject(ctx, route, parameters)
		if err != nil {
			return nil, err
		}

		jobs.AddJobs(db, &async.JobQualifier{
			ViewName:         route.View.Name,
			PrincipalSubject: subject,
		})
	}

	return jobs.Index(), nil
}

func (r *Router) executorPayloadReader(ctx context.Context, response http.ResponseWriter, request *http.Request, route *Route) (PayloadReader, error) {
	anExecutor := NewExecutor(route, request, nil, response)
	if route.Handler != nil {
		sessionHandler, err := anExecutor.SessionHandler(ctx)
		if err != nil {
			return nil, err
		}

		res, err := route.Handler.Call(ctx, sessionHandler)
		if err != nil {
			return nil, err
		}

		if err = anExecutor.Execute(ctx); err != nil {
			return nil, err
		}

		return r.marshalCustomOutput(res, route)
	}

	sess, err := anExecutor.ExpandAndExecute(ctx)
	if err != nil {
		return nil, err
	}

	if route.ResponseBody == nil {
		return NewBytesReader(nil, ""), nil
	}

	params, _ := anExecutor.RequestParams(ctx)
	body, err := route.execResponseBody(params, sess)
	if err != nil {
		return nil, err
	}

	responseBody := r.wrapWithResponseIfNeeded(body, route, nil, nil, sess.State)

	marshal, err := route._jsonMarshaller.Marshal(responseBody)
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return NewBytesReader(marshal, ""), nil
}

func (r *Router) prepareExecutorSessionWithParameters(ctx context.Context, request *http.Request, route *Route, parameters *RequestParams) (*executor.Session, error) {
	selectors, _, err := CreateSelectorsFromRoute(ctx, route, request, parameters, route.Index._viewDetails...)
	if err != nil {
		return nil, err
	}

	sess, err := executor.NewSession(selectors, route.View)
	return sess, err
}

func (r *Router) asyncConnector(route *Route, record *async2.Job) (*view.Connector, error) {
	if record.DestinationConnector != "" {
		return r._resource.Resource.Connector(record.DestinationConnector)
	}

	if route.Async != nil && route.Async.Connector != nil {
		return route.Async.Connector, nil
	}

	return nil, fmt.Errorf("unspecified job connector")
}

func (r *Route) NewStater(request *http.Request, parameters *RequestParams) *Stater {
	return &Stater{
		route:      r,
		request:    request,
		parameters: parameters,
		cache:      r._stateCache,
		resource:   r._resource,
	}
}

func (r *Router) prepareAndExecuteExecutor(ctx context.Context, request *http.Request, route *Route, parameters *RequestParams) error {
	execSession, err := r.prepareExecutorSessionWithParameters(ctx, request, route, parameters)
	if err != nil {
		return err
	}

	anExecutor := executor.New()
	err = anExecutor.Exec(ctx, execSession)
	if err != nil {
		return err
	}

	return nil
}

func (r *Router) marshalAsXLS(session *ReaderSession, readerSession *preparedResponse, filters []*json.FilterEntry) ([]byte, error) {
	return session.Route.XLS._xlsMarshaller.Marshal(readerSession.objects)
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

func (r *Route) execResponseBody(parameters *RequestParams, session *executor.Session) (interface{}, error) {
	if r.ResponseBody != nil {
		return r.ResponseBody.getValue(session)
	}

	return parameters.RequestBody()
}
