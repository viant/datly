package router

import (
	"bytes"
	"context"
	goJson "encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	acontent "github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/openapi"
	"github.com/viant/datly/gateway/router/status"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
)

// ContextHandler http handler with context
type ContextHandler func(ctx context.Context, response http.ResponseWriter, request *http.Request)

const (
	Separator = ", "
)

type (
	Handler struct {
		Path       *path.Path
		Provider   *repository.Provider
		dispatcher *operator.Service
		registry   *repository.Registry
	}
)

func (r *Handler) HandleRequest(ctx context.Context, response http.ResponseWriter, request *http.Request) {
	err := r.AuthorizeRequest(request, r.Path)
	if err != nil {
		httputils.WriteError(response, err)
		return
	}
	if request.Method == http.MethodOptions {
		CorsHandler(request, r.Path.Cors)(response)
		return
	}
	if r.Path.Cors != nil {
		CorsHandler(request, r.Path.Cors)(response)
	}
	r.Handle(ctx, response, request)

}

func (r *Handler) AuthorizeRequest(request *http.Request, aPath *path.Path) error {
	apiKey := aPath.APIKey
	if apiKey == nil {
		return nil
	}
	key := request.Header.Get(apiKey.Header)
	if key != apiKey.Value {
		return response.NewError(http.StatusUnauthorized, "")
	}

	return nil
}

func New(aPath *path.Path, provider *repository.Provider, registry *repository.Registry) *Handler {
	ret := &Handler{
		Path:       aPath,
		Provider:   provider,
		dispatcher: operator.New(),
		registry:   registry,
	}
	return ret
}

func CorsHandler(request *http.Request, cors *path.Cors) func(writer http.ResponseWriter) {
	return func(writer http.ResponseWriter) {
		enableCors(writer, request, cors, true)
	}
}

func enableCors(writer http.ResponseWriter, request *http.Request, cors *path.Cors, allHeaders bool) {
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
	if request.Method == "OPTIONS" {
		requestMethod := request.Header.Get(httputils.AllControlRequestHeader)
		if requestMethod != "" {
			writer.Header().Set(httputils.AllowMethodsHeader, requestMethod)
		}
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

func (r *Handler) Serve(serverPath string) error {
	return http.ListenAndServe(serverPath, r)
}

func (r *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := context.Background()
	execContext := exec.NewContext()
	ctx = context.WithValue(ctx, exec.ContextKey, execContext)
	r.HandleRequest(ctx, writer, request)
}

func (r *Handler) Handle(ctx context.Context, writer http.ResponseWriter, request *http.Request) {
	aComponent, err := r.Provider.Component(ctx)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	aResponse, err := r.safelyHandleComponent(ctx, request, aComponent)
	if err != nil {
		r.writeErrorResponse(writer, aComponent, err, http.StatusBadRequest)
		return
	}

	r.writeResponse(ctx, request, writer, aComponent, aResponse)
}

func (r *Handler) safelyHandleComponent(ctx context.Context, request *http.Request, aComponent *repository.Component) (aResponse response.Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			stackLines := strings.Split(string(debug.Stack()), "\n")
			stackInfo := extractPanicInfo(stackLines)
			err = response.NewError(http.StatusInternalServerError, fmt.Sprintf("failed to handle request %v, %s", r, stackInfo))
		}
	}()
	return r.handleComponent(ctx, request, aComponent)
}

func extractPanicInfo(lines []string) interface{} {
	var postPanic []string
	hadPanic := false
	for i := 0; i < len(lines); i++ {
		if strings.Contains(lines[i], "panic") || strings.Contains(lines[i], "nil") {
			hadPanic = true
		}
		if hadPanic {
			postPanic = append(postPanic, strings.TrimSpace(lines[i]))

		}
	}

	if len(postPanic) > 5 {
		postPanic = postPanic[:5]
	}
	return strings.Join(postPanic, "\n")
}

func (r *Handler) writeErrorResponse(w http.ResponseWriter, aComponent *repository.Component, err error, statusCode int) {
	statusCode, message, anObjectErr := status.NormalizeErr(err, statusCode)
	if statusCode < http.StatusBadRequest {
		statusCode = http.StatusBadRequest
	}
	responseStatus := r.responseStatusError(message, anObjectErr)
	statusParameter := aComponent.Output.Type.Parameters.LookupByLocation(state.KindOutput, "status")
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

	outputType := aComponent.Output.Type
	var data []byte
	if outputType.Type() != nil {
		aResponse := aComponent.Output.Type.Type().NewState()
		if err = aResponse.SetValue(statusParameter.Name, responseStatus); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err = aComponent.Marshaller.JSON.JsonMarshaller.Marshal(aResponse.State())
		if err != nil {
			w.Write(data)
			w.WriteHeader(statusCode)
			return
		}
	}
	w.WriteHeader(statusCode)
	if len(data) > 0 {
		w.Write(data)
	}
}

func (r *Handler) responseStatusError(message string, anObject interface{}) response.Status {
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

func (r *Handler) writeResponse(ctx context.Context, request *http.Request, writer http.ResponseWriter, aComponent *repository.Component, aResponse response.Response) {
	redirected, err := r.redirectIfNeeded(ctx, request, writer, aComponent, aResponse)
	if redirected {
		return
	}
	if err != nil {
		r.writeErrorResponse(writer, aComponent, err, http.StatusInternalServerError)
		return
	}
	if size := aResponse.Size(); size > 0 {
		writer.Header().Add(httputils.ContentLength, strconv.Itoa(aResponse.Size()))
	}
	for key, value := range aResponse.Headers() {
		writer.Header().Add(key, value[0])
	}
	compressed, ok := aResponse.(response.Compressed)
	if ok && compressed.CompressionType() != "" {
		writer.Header().Add(acontent.Encoding, compressed.CompressionType())
	}
	statusCode := http.StatusOK
	if aResponse.StatusCode() > 0 {
		statusCode = aResponse.StatusCode()
	}
	execCtx := exec.GetContext(ctx)
	if execCtx != nil && execCtx.StatusCode != 0 {
		writer.WriteHeader(execCtx.StatusCode)
		execCtx.StatusCode = -1
	} else {
		writer.WriteHeader(statusCode)
	}
	if reader := aResponse.Body(); reader != nil {
		_, _ = io.Copy(writer, reader)
	}
}

func (r *Handler) PreSign(ctx context.Context, viewName string, aResponse response.Response) (*option.PreSign, error) {
	redirect := r.Path.Redirect
	fs := afs.New()
	UUID := uuid.New()
	URL := url.Join(redirect.StorageURL, normalizeStorageURL(viewName), normalizeStorageURL(UUID.String())) + ".json"
	preSign := option.NewPreSign(redirect.TimeToLive())
	kv := []string{acontent.Type, httputils.ContentTypeJSON}

	compressed, ok := aResponse.(response.Compressed)
	if ok {
		return nil, fmt.Errorf("response is not compressed")
	}
	compressionType := compressed.CompressionType()
	if compressionType != "" {
		kv = append(kv, acontent.Encoding, compressionType)
	}
	meta := acontent.NewMeta(kv...)
	err := fs.Upload(ctx, URL, file.DefaultFileOsMode, aResponse.Body(), preSign, meta)
	return preSign, err
}

func (r *Handler) redirectIfNeeded(ctx context.Context, request *http.Request, response http.ResponseWriter, aComponent *repository.Component, aResponse response.Response) (redirected bool, err error) {
	redirect := r.Path.Redirect
	if redirect == nil {
		return false, nil
	}

	if redirect.MinSizeKb*1024 > aResponse.Size() {
		return false, nil
	}
	preSign, err := r.PreSign(ctx, aComponent.View.Name, aResponse)
	if err != nil {
		return false, err
	}
	http.Redirect(response, request, preSign.URL, http.StatusMovedPermanently)
	return true, nil
}

func (r *Handler) compressIfNeeded(marshalled []byte, options *response.Options) (response.Response, error) {
	compression := r.Path.Compression
	if compression == nil || (compression.MinSizeKb > 0 && len(marshalled) <= compression.MinSizeKb*1024) {
		options.Append(response.WithBytes(marshalled))
		return response.NewBuffered(options.Options()...), nil
	}
	buffer, err := httputils.Compress(bytes.NewReader(marshalled))
	if err != nil {
		return nil, response.NewError(http.StatusInternalServerError, err.Error(), response.WithError(err))
	}
	options.Append(response.WithBuffer(buffer), response.WithCompressions(httputils.EncodingGzip))
	return response.NewBuffered(options.Options()...), nil
}

func (r *Handler) logAudit(request *http.Request, response http.ResponseWriter, aPath *path.Path) {
	headers := request.Header.Clone()
	Sanitize(request, aPath, headers, response)

	asBytes, _ := goJson.Marshal(path.Audit{
		URL:     request.RequestURI,
		Headers: headers,
	})
	fmt.Printf("%v\n", string(asBytes))
}

func (r *Handler) logMetrics(URI string, metrics []*response.Metric) {
	asBytes, _ := goJson.Marshal(NewMetrics(URI, metrics))
	fmt.Printf("%v\n", string(asBytes))
}

func (r *Handler) handleComponent(ctx context.Context, request *http.Request, aComponent *repository.Component) (response.Response, error) {
	//TODO merge with Path settings

	anOperator := operator.New()
	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := append(aComponent.LocatorOptions(request, hstate.NewForm(), unmarshal))
	aSession := session.New(aComponent.View, session.WithLocatorOptions(locatorOptions...), session.WithRegistry(r.registry), session.WithOperate(anOperator.Operate))
	err := aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindRequestBody, state.KindForm, state.KindQuery)
	if err != nil {
		return nil, err
	}
	if ctx, err = r.dispatcher.EnsureContext(ctx, aSession, aComponent); err != nil {
		return nil, err
	}
	if err := aSession.Populate(ctx); err != nil {
		return nil, err
	}
	output, operationErr := r.dispatcher.Operate(ctx, aSession, aComponent)
	if operationErr != nil && output == nil {
		return nil, operationErr
	}
	if redirect := aSession.Redirect; redirect != nil {
		aSession.Redirect = nil //reset redirect
		provider, err := r.registry.LookupProvider(ctx, contract.NewPath(redirect.Route.Method, redirect.Route.URL))
		if err != nil {
			return nil, err
		}
		redirectingComponent, err := provider.Component(ctx)
		if err != nil {
			return nil, err
		}
		return r.handleComponent(ctx, redirect.Request, redirectingComponent)
	}

	//TODO: add redirect option
	//get matched compoent, and requeest
	//return handleComponent(ctx, request, aComponent)

	options := &response.Options{}
	options.AdjustStatusCode(output, operationErr)
	if output == nil {
		return response.NewBuffered(options.Options()...), nil
	}
	if aComponent.Service == service.TypeReader {
		format := aComponent.Output.Format(request.URL.Query())
		contentType := aComponent.Output.ContentType(format)
		options.Append(response.WithHeader("Content-Type", contentType))
		if aComponent.Output.Title != "" {
			switch format {
			case content.XLSFormat:
				options.Append(response.WithHeader("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.xlsx"`, aComponent.Output.GetTitle())))
			}
		}
		filters := aComponent.Exclusion(aSession.State())
		data, err := aComponent.Content.Marshal(format, aComponent.Output.Field(), output, filters)
		if err != nil {
			return nil, response.NewError(500, fmt.Sprintf("failed to marshal response: %v", err), response.WithError(err))
		}
		return r.compressIfNeeded(data, options)
	}
	return r.marshalComponentOutput(output, aComponent, options)
}

func (r *Handler) marshalComponentOutput(output interface{}, aComponent *repository.Component, options *response.Options) (response.Response, error) {
	switch actual := output.(type) {
	case response.Response:
		return actual, nil
	case []byte:
		return response.NewBuffered(response.WithBytes(actual)), nil
	default:
		data, err := aComponent.Content.Marshaller.JSON.JsonMarshaller.Marshal(output)
		if err != nil {
			return nil, response.NewError(http.StatusInternalServerError, err.Error(), response.WithError(err))
		}
		options.Append(response.WithHeader(HeaderContentType, openapi.ApplicationJson))
		return r.compressIfNeeded(data, options)
	}
}

func ExtractCacheableViews(ctx context.Context, component *repository.Component) ([]*view.View, error) {
	var views []*view.View
	appendCacheWarmupViews(component.View, &views)
	return views, nil
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

func normalizeStorageURL(part string) string {
	part = strings.ReplaceAll(part, "-", "")
	part = strings.ReplaceAll(part, "_", "")
	return part
}
