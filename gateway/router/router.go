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
	"github.com/viant/datly/gateway/router/status"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/dispatcher"
	expand2 "github.com/viant/datly/service/executor/expand"
	reader2 "github.com/viant/datly/service/reader"
	session "github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	haHttp "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// TODO: Add to meta response size
type viewHandler func(ctx context.Context, response http.ResponseWriter, request *http.Request)

const (
	Separator = ", "
)

type (
	Router struct {
		dispatcher *dispatcher.Service
		Matcher    *matcher.Matcher
		_mux       sync.Mutex
		_resource  *Resource
		_index     map[string][]int
		_routes    Routes
	}

	BytesReadCloser struct {
		bytes *bytes.Buffer
	}

	MatchableRoute struct {
		Route *Route
	}

	ApiPrefix string
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

func (r *Route) IsMetricsEnabled(req *http.Request) bool {
	return r.IsMetricInfo(req) || r.IsMetricDebug(req)
}

func (r *Route) IsMetricInfo(req *http.Request) bool {
	if !r.Output.IsRevealMetric() {
		return false
	}
	value := req.Header.Get(httputils.DatlyRequestMetricsHeader)
	if value == "" {
		value = req.Header.Get(strings.ToLower(httputils.DatlyRequestMetricsHeader))
	}
	return strings.ToLower(value) == httputils.DatlyInfoHeaderValue
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
	return r.HandleAsync(context.Background(), response, request)
}

func (r *Router) HandleAsync(ctx context.Context, response http.ResponseWriter, request *http.Request) error {
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
	return r.HandleRequest(ctx, response, request, route.(*MatchableRoute).Route)
}

func (r *Router) HandleRoute(ctx context.Context, response http.ResponseWriter, request *http.Request, route *Route) error {
	return r.HandleRequest(ctx, response, request, route)
}

func (r *Router) HandleRequest(ctx context.Context, response http.ResponseWriter, request *http.Request, route *Route) error {
	err := r.AuthorizeRequest(request, route)
	if err != nil {
		httputils.WriteError(response, err)
		return nil
	}

	if request.Method == http.MethodOptions {
		corsHandler(request, route.Cors)(response)
		return nil
	}

	r.viewHandler(route)(ctx, response, request)
	if route.Cors != nil {
		corsHandler(request, route.Cors)(response)
	}
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
		_resource:  resource,
		dispatcher: dispatcher.New(),
		_index:     map[string][]int{},
		_routes:    resource.Routes,
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

	err = r.HandleRoute(context.Background(), writer, request, route.(*MatchableRoute).Route)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {

	return func(ctx context.Context, response http.ResponseWriter, request *http.Request) {

		payloadReader, err := r.payloadReader(ctx, request, response, route)
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

	aResponse := route.Output.Type.Type().NewState()

	if err = aResponse.SetValue(statusParameter.Name, responseStatus); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	asBytes, marErr := route.JsonMarshaller.Marshal(aResponse.State())
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

	asEmbeddable, ok := anObject.(expand2.EmbeddableMap)
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

func (r *Router) compressIfNeeded(marshalled []byte, route *Route, option ...RequestDataReaderOption) (*RequestDataReader, error) {
	compression := route.Compression

	if compression == nil || (compression.MinSizeKb > 0 && len(marshalled) <= compression.MinSizeKb*1024) {
		return NewBytesReader(marshalled, "", option...), nil
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

func (r *Router) Interceptor() (*RouteInterceptor, bool) {
	return r._resource.Interceptor, r._resource.Interceptor != nil
}

func (r *Router) Resource() *Resource {
	return r._resource
}

func (r *Router) payloadReader(ctx context.Context, request *http.Request, writer http.ResponseWriter, route *Route) (PayloadReader, error) {
	unmarshal := route.UnmarshalFunc(request)

	locatorOptions := append(route.LocatorOptions(request, unmarshal), locator.WithDispatcher(route.dispatcher))
	aSession := session.New(route.View, session.WithLocatorOptions(locatorOptions...))
	err := aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindQuery, state.KindRequestBody)
	if err != nil {
		return nil, err
	}
	if err := aSession.Populate(ctx); err != nil {
		return nil, err
	}
	aResponse, err := r.dispatcher.Dispatch(ctx, &route.Component, aSession)
	if err != nil {
		return nil, err
	}
	if aResponse == nil {
		return NewBytesReader(nil, ""), nil
	}

	if route.Service == service.TypeReader {
		format := route.Output.Format(request.URL.Query())
		contentType := route.Output.ContentType(format)
		filters := route.Exclusion(aSession.State())
		data, err := route.Marshal(format, route.Output.Field, aResponse, filters)
		if err != nil {
			return nil, httputils.NewHttpMessageError(500, fmt.Errorf("failed to marshal response: %w", err))
		}

		//WithHeader
		return r.compressIfNeeded(data, route, WithHeader("Content-Type", contentType))
	}
	return r.marshalCustomOutput(aResponse, route)
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
