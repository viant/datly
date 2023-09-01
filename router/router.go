package router

import (
	"bytes"
	"context"
	"encoding/base64"
	goJson "encoding/json"
	"fmt"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	content2 "github.com/viant/datly/router/content"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/status"
	"github.com/viant/datly/service"
	executor2 "github.com/viant/datly/service/executor"
	reader2 "github.com/viant/datly/service/reader"
	rhandler "github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	vsession "github.com/viant/datly/view/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/toolbox"
	async2 "github.com/viant/xdatly/handler/async"
	haHttp "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	}

	BytesReadCloser struct {
		bytes *bytes.Buffer
	}

	MatchableRoute struct {
		Route *Route
	}

	ApiPrefix string

	preparedResponse struct {
		objects  interface{}
		viewMeta interface{}
		session  *reader2.Session
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
	return s.Route.Output.DebugKind == view.MetaTypeHeader || (s.IsMetricInfo() || s.IsMetricDebug())
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
	if route.CSV == nil && requestParams.OutputContentType == content2.CSVContentType {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output CSV config?)", content2.CSVContentType)))
	}
	if route.TabularJSON == nil && route.Output.DataFormat == content2.JSONDataFormatTabular {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output DataFormat config?)", content2.JSONContentType)))
	}
	if route.XML == nil && route.Output.DataFormat == content2.XMLFormat {
		return nil, httputils.NewHttpMessageError(http.StatusBadRequest, UnsupportedFormatErr(fmt.Sprintf("%s (forgotten output DataFormat config?)", content2.XMLContentType)))
	}

	sessionState := vsession.New(route.View, vsession.WithLocatorOptions(route.LocatorOptions(request)...))
	if err := sessionState.Populate(ctx); err != nil {
		defaultCode := http.StatusBadRequest
		return nil, httputils.ErrorOf(defaultCode, err)
	}

	return &ReaderSession{
		RequestParams: requestParams,
		Route:         route,
		Request:       request,
		Response:      response,
		State:         sessionState.State(),
	}, nil
}

func UnsupportedFormatErr(format string) error {
	return fmt.Errorf("unsupported output format %v", format)
}

func (r *Router) inAWS() bool {
	scheme := url.Scheme(r._resource.SourceURL, "s3")
	return scheme == "s3"
}

func (r *Router) writeErr(w http.ResponseWriter, route *Route, err error, statusCode int) {
	statusCode, message, anObjectErr := status.NormalizeErr(err, statusCode)
	if statusCode < http.StatusBadRequest {
		statusCode = http.StatusBadRequest
	}

	responseStatus := r.responseStatusError(message, anObjectErr)
	statusParameter := route.Output.Type.Parameters.LookupByLocation(state.KindOutput, "status")

	if statusParameter == nil {
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

	response := route.Output.Type.Type().NewState()
	response.SetValue(statusParameter.Name, responseStatus)

	asBytes, marErr := route.JsonMarshaller.Marshal(response.State())
	if marErr != nil {
		w.Write(asBytes)
		w.WriteHeader(statusCode)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(asBytes)
}

func (r *Router) responseStatusError(message string, anObject interface{}) response.Status {
	responseStatus := response.Status{
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

func (r *Router) logMetrics(URI string, metrics []*reader2.Metric) {
	asBytes, _ := goJson.Marshal(NewMetrics(URI, metrics))

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

func (r *Router) responseStatusSuccess(state *expand.State) response.Status {
	status := response.Status{Status: "ok"}
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
	case service.TypeExecutor:
		return r.executorPayloadReader(ctx, response, request, route)
	case service.TypeReader:
		sessionState := vsession.New(route.View, vsession.WithLocatorOptions(route.LocatorOptions(request)...))
		readerHandler := rhandler.New(route.Output.Type.Type(), &route.Output.Type)
		aResponse := readerHandler.Handle(ctx, route.View, sessionState,
			reader2.WithIncludeSQL(true),
			reader2.WithCacheDisabled(false))
		if aResponse.Error != nil {
			return nil, aResponse.Error
		}
		format := route.OutputFormat(request.URL.Query())
		filters := route.Exclusion(sessionState.State())
		data, err := route.Content.Marshal(format, route.Output.Field, aResponse.Reader.Data, aResponse.Output, filters)
		if err != nil {
			return nil, err
		}
		return r.compressIfNeeded(data, route)
	}
	return nil, httputils.NewHttpMessageError(500, fmt.Errorf("unsupported Type %v", route.Service))
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
		marshal, err := route.JsonMarshaller.Marshal(output)
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
		return route.JsonMarshaller.Marshal(route, responseValue)
	}
}

func (r *Router) executorPayloadReader(ctx context.Context, writer http.ResponseWriter, request *http.Request, route *Route) (PayloadReader, error) {
	anExecutor := NewExecutor(route, request, writer)
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
	aSession, err := anExecutor.ExpandAndExecute(ctx)

	if err != nil {
		return nil, err
	}
	if route.Output.ResponseBody == nil {
		return NewBytesReader(nil, ""), nil
	}

	var responseValue interface{}
	if stateType := route.Output.Type.Type(); stateType != nil && stateType.IsDefined() {
		responseState := route.Output.Type.Type().NewState()
		sessionState := aSession.SessionState
		statelet := aSession.SessionState.State().Lookup(aSession.View)

		status := r.responseStatusSuccess(aSession.TemplateState)
		sessionState.SetState(ctx, route.Output.Type.Parameters, responseState, sessionState.Indirect(true,
			locator.WithCustomOption(&status),
			locator.WithState(statelet.Template)))

		responseValue = responseState.State()

		if parameter := route.Output.Type.AnonymousParameters(); parameter != nil {
			if responseValue, err = responseState.Value(parameter.Name); err != nil {
				return nil, err
			}
		}
	}

	toolbox.Dump(responseValue)
	if err != nil {
		return nil, err
	}
	data, err := route.JsonMarshaller.Marshal(responseValue)
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}

	return NewBytesReader(data, ""), nil
}

func (r *Router) prepareExecutorSessionWithParameters(ctx context.Context, request *http.Request, route *Route, parameters *RequestParams) (*executor2.Session, error) {
	sessionState := vsession.New(route.View, vsession.WithLocatorOptions(route.LocatorOptions(request)...))
	if err := sessionState.Populate(ctx); err != nil {
		return nil, err
	}
	sess, err := executor2.NewSession(sessionState, route.View)
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

func (r *Router) prepareAndExecuteExecutor(ctx context.Context, request *http.Request, route *Route, parameters *RequestParams) error {
	execSession, err := r.prepareExecutorSessionWithParameters(ctx, request, route, parameters)
	if err != nil {
		return err
	}

	anExecutor := executor2.New()
	err = anExecutor.Exec(ctx, execSession)
	if err != nil {
		return err
	}

	return nil
}

func (r *Router) marshalAsXLS(session *ReaderSession, readerSession *preparedResponse) ([]byte, error) {
	return session.Route.Content.Marshaller.XLS.XlsMarshaller.Marshal(readerSession.objects)
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
