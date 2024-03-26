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
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler/exec"
	haHttp "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	"io"
	"net/http"
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
		return httputils.NewHttpMessageError(http.StatusUnauthorized, nil)
	}

	return nil
}

func New(aPath *path.Path, provider *repository.Provider) *Handler {
	ret := &Handler{
		Path:       aPath,
		Provider:   provider,
		dispatcher: operator.New(),
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
	ctx = context.WithValue(ctx, exec.ContextKey, exec.NewContext())
	r.HandleRequest(ctx, writer, request)
}

func (r *Handler) Handle(ctx context.Context, response http.ResponseWriter, request *http.Request) {
	aComponent, err := r.Provider.Component(ctx)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	payloadReader, err := r.handleWithComponent(ctx, request, response, aComponent)
	if err != nil {
		code, _ := httputils.BuildErrorResponse(err)
		r.writeErr(response, aComponent, err, code)
		return
	}
	if payloadReader != nil {
		r.writeResponse(ctx, request, response, aComponent, payloadReader)
	}
}

func (r *Handler) writeErr(w http.ResponseWriter, aComponent *repository.Component, err error, statusCode int) {
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

	aResponse := aComponent.Output.Type.Type().NewState()
	if err = aResponse.SetValue(statusParameter.Name, responseStatus); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	asBytes, marErr := aComponent.Marshaller.JSON.JsonMarshaller.Marshal(aResponse.State())
	if marErr != nil {
		w.Write(asBytes)
		w.WriteHeader(statusCode)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(asBytes)
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

func (r *Handler) writeResponse(ctx context.Context, request *http.Request, response http.ResponseWriter, aComponent *repository.Component, payloadReader PayloadReader) {
	defer payloadReader.Close()
	redirected, err := r.redirectIfNeeded(ctx, request, response, aComponent, payloadReader)
	if redirected {
		return
	}

	if err != nil {
		r.writeErr(response, aComponent, err, http.StatusInternalServerError)
		return
	}

	response.Header().Add(httputils.ContentLength, strconv.Itoa(payloadReader.Size()))
	for key, value := range payloadReader.Headers() {
		response.Header().Add(key, value[0])
	}

	compressionType := payloadReader.CompressionType()
	if compressionType != "" {
		response.Header().Set(acontent.Encoding, compressionType)
	}

	response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(response, payloadReader)
}

func (r *Handler) PreSign(ctx context.Context, viewName string, payload PayloadReader) (*option.PreSign, error) {
	redirect := r.Path.Redirect
	fs := afs.New()
	UUID := uuid.New()
	URL := url.Join(redirect.StorageURL, normalizeStorageURL(viewName), normalizeStorageURL(UUID.String())) + ".json"
	preSign := option.NewPreSign(redirect.TimeToLive())
	kv := []string{acontent.Type, httputils.ContentTypeJSON}
	compressionType := payload.CompressionType()

	if compressionType != "" {
		kv = append(kv, acontent.Encoding, compressionType)
	}
	meta := acontent.NewMeta(kv...)
	err := fs.Upload(ctx, URL, file.DefaultFileOsMode, payload, preSign, meta)
	return preSign, err
}

func (r *Handler) redirectIfNeeded(ctx context.Context, request *http.Request, response http.ResponseWriter, aComponent *repository.Component, payloadReader PayloadReader) (redirected bool, err error) {
	redirect := r.Path.Redirect
	if redirect == nil {
		return false, nil
	}

	if redirect.MinSizeKb*1024 > payloadReader.Size() {
		return false, nil
	}
	preSign, err := r.PreSign(ctx, aComponent.View.Name, payloadReader)
	if err != nil {
		return false, err
	}
	http.Redirect(response, request, preSign.URL, http.StatusMovedPermanently)
	return true, nil
}

func (r *Handler) compressIfNeeded(marshalled []byte, option ...RequestDataReaderOption) (*RequestDataReader, error) {
	compression := r.Path.Compression
	if compression == nil || (compression.MinSizeKb > 0 && len(marshalled) <= compression.MinSizeKb*1024) {
		return NewBytesReader(marshalled, "", option...), nil
	}
	buffer, err := httputils.Compress(bytes.NewReader(marshalled))
	if err != nil {
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
	}
	payloadSize := buffer.Len()

	//TODO address this aws response adapter
	//if r.inAWS() {
	//	payloadSize = base64.StdEncoding.EncodedLen(payloadSize)
	//}
	return AsBytesReader(buffer, httputils.EncodingGzip, payloadSize), nil
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

func (r *Handler) handleWithComponent(ctx context.Context, request *http.Request, writer http.ResponseWriter, aComponent *repository.Component) (PayloadReader, error) {

	//TODO merge with Path settings
	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := append(aComponent.LocatorOptions(request, state.NewForm(), unmarshal))
	aSession := session.New(aComponent.View, session.WithLocatorOptions(locatorOptions...))
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
	aResponse, err := r.dispatcher.Operate(ctx, aSession, aComponent)
	if err != nil {
		return nil, err
	}
	if aResponse == nil {
		return NewBytesReader(nil, ""), nil
	}

	if aComponent.Service == service.TypeReader {
		format := aComponent.Output.Format(request.URL.Query())
		contentType := aComponent.Output.ContentType(format)
		var options []RequestDataReaderOption
		options = append(options, WithHeader("Content-Type", contentType))
		if aComponent.Output.Title != "" {
			switch format {
			case content.XLSFormat:
				options = append(options, WithHeader("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.xlsx"`, aComponent.Output.GetTitle())))
			}
		}
		filters := aComponent.Exclusion(aSession.State())
		data, err := aComponent.Content.Marshal(format, aComponent.Output.Field, aResponse, filters)
		if err != nil {
			return nil, httputils.NewHttpMessageError(500, fmt.Errorf("failed to marshal response: %w", err))
		}
		return r.compressIfNeeded(data, options...)
	}

	return r.marshalCustomOutput(aResponse, aComponent)
}

func (r *Handler) marshalCustomOutput(output interface{}, aComponent *repository.Component) (PayloadReader, error) {
	switch actual := output.(type) {
	case haHttp.Response:
		responseContent, err := r.extractValueFromResponse(aComponent, actual)
		if err != nil {
			return nil, err
		}
		return NewBytesReader(responseContent, "", WithHeaders(actual.Headers())), nil
	case []byte:
		return NewBytesReader(actual, ""), nil
	default:
		marshal, err := aComponent.Content.Marshaller.JSON.JsonMarshaller.Marshal(output)
		if err != nil {
			return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, err)
		}
		return NewBytesReader(marshal, "", WithHeader(HeaderContentType, openapi.ApplicationJson)), nil
	}
}

func (r *Handler) extractValueFromResponse(aComponent *repository.Component, actual haHttp.Response) ([]byte, error) {
	value := actual.Value()
	switch responseValue := value.(type) {
	case []byte:
		return responseValue, nil
	default:
		return aComponent.Content.Marshaller.JSON.JsonMarshaller.Marshal(responseValue)
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
