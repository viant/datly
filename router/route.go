package router

import (
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/content"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/datly/view/template"
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	async2 "github.com/viant/xdatly/handler/async"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xunsafe"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
)

type Style string
type ServiceType string

const pkgPath = "github.com/viant/datly/router"

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"

	ServiceTypeReader   ServiceType = "Reader"
	ServiceTypeExecutor ServiceType = "Executor"

	ServiceTypeHandler ServiceType = "Handler"

	HeaderContentType = "Content-Type"

	FormatQuery = "_format"
)

type (
	Routes []*Route
	Route  struct {
		Async            *Async             `json:",omitempty" yaml:",omitempty"`
		Name             string             `json:",omitempty" yaml:",omitempty"`
		URI              string             `json:",omitempty"`
		APIKey           *APIKey            `json:",omitempty"`
		Method           string             `json:",omitempty"`
		CustomValidation bool               `json:",omitempty"`
		Service          ServiceType        `json:",omitempty"`
		View             *view.View         `json:",omitempty"`
		Cors             *Cors              `json:",omitempty"`
		EnableAudit      bool               `json:",omitempty"`
		EnableDebug      *bool              `json:",omitempty"`
		Transforms       marshal.Transforms `json:",omitempty"`

		Input
		content.Content
		Output

		*view.NamespacedView

		ParamStatusError *int         `json:",omitempty"`
		Compression      *Compression `json:",omitempty"`
		Handler          *Handler     `json:",omitempty"`

		_unmarshallerInterceptors marshal.Transforms

		_resource *view.Resource

		_requestBodyParamRequired bool
		_requestBodyType          reflect.Type
		_requestBodySlice         *xunsafe.Slice
		_apiKeys                  []*APIKey
		_stateCache               *staterCache
		_routeMatcher             func(route *http2.Route) (*Route, error)
		_async                    *async.Async
		_router                   *Router
	}

	Input struct {
		RequestBodySchema *state.Schema
	}

	Output struct {
		Cardinality      state.Cardinality    `json:",omitempty"`
		CaseFormat       formatter.CaseFormat `json:",omitempty"`
		OmitEmpty        bool                 `json:",omitempty"`
		Style            Style                `json:",omitempty"`
		Field            string               `json:",omitempty"`
		Exclude          []string
		NormalizeExclude *bool

		RevealMetric *bool
		DebugKind    view.MetaKind

		DataFormat string `json:",omitempty"` //default data format

		ResponseBody *BodySelector

		Schema     *state.Schema
		Parameters state.Parameters
		Type       *structology.StateType

		_caser          *format.Case
		_excluded       map[string]bool
		_responseSetter *responseSetter
	}

	responseSetter struct {
		statusField *xunsafe.Field
		bodyField   *xunsafe.Field
		metaField   *xunsafe.Field
		infoField   *xunsafe.Field
		debug       *xunsafe.Field
		rType       reflect.Type
	}
)

func (r *Route) Exclusion(state *view.ResourceState) []*json.FilterEntry {
	result := make([]*json.FilterEntry, 0)
	state.Lock()
	defer state.Unlock()
	for viewName, selector := range state.Views {
		if len(selector.Columns) == 0 {
			continue
		}
		var aPath string
		nsView := r.NamespacedView.ByName(viewName)
		if nsView == nil {
			aPath = ""
		} else {
			aPath = nsView.Path
		}
		fields := make([]string, len(selector.Fields))
		for i := range selector.Fields {
			fields[i] = selector.Fields[i]
		}
		result = append(result, &json.FilterEntry{
			Path:   aPath,
			Fields: fields,
		})

	}
	return result
}

// OutputFormat returns output foramt
func (r *Route) OutputFormat(query url.Values) string {
	outputFormat := query.Get(FormatQuery)
	if outputFormat == "" {
		outputFormat = r.Output.DataFormat
	}
	if outputFormat == "" {
		outputFormat = content.JSONFormat
	}
	return outputFormat
}

func (r *Route) IsRevealMetric() bool {
	if r.RevealMetric == nil {
		return false
	}
	return *r.RevealMetric
}

func (r *Route) HttpURI() string {
	return r.URI
}

func (r *Route) Marshaller(request *http.Request) *marshal.Marshaller {
	contentType := request.Header.Get(HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(HeaderContentType)))
	switch contentType {
	case content.CSVContentType:
		return marshal.NewMarshaller(r._requestBodySlice.Type, r.CSV.Unmarshal)
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	for i := range r._unmarshallerInterceptors {
		transform := r._unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = r.transformFn(request, transform)
	}
	return marshal.NewMarshaller(r._requestBodyType, func(bytes []byte, i interface{}) error {
		return r.JsonMarshaller.Unmarshal(bytes, i, jsonPathInterceptor, request)
	})
}

func (r *Route) LocatorOptions(request *http.Request) []locator.Option {
	var result []locator.Option
	marshaller := r.Marshaller(request)
	result = append(result, locator.WithUnmarshal(marshaller.Unmarshal))
	result = append(result, locator.WithRequest(request))
	result = append(result, locator.WithURIPattern(r.URI))
	result = append(result, locator.WithIOConfig(r.ioConfig()))
	result = append(result, locator.WithParameters(r._resource.NamedParameters()))
	if r.RequestBodySchema != nil {
		result = append(result, locator.WithBodyType(r.RequestBodySchema.Type()))
	}
	if r._resource != nil {
		result = append(result, locator.WithViews(r._resource.Views.Index()))
	}
	return result
}

func (r *Route) HttpMethod() string {
	return r.Method
}

func (r *Route) CorsEnabled() bool {
	return r.Cors != nil
}

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if r.Style == BasicStyle {
		r.Field = ""
	}
	if r.Handler != nil {
		if err := r.Handler.Init(ctx, resource.Resource); err != nil {
			return err
		}
	}
	if err := r.initCardinality(); err != nil {
		return err
	}
	r.View.Standalone = true
	if r.View.Name == "" {
		r.View.Name = r.View.Ref
	}

	if err := r.initView(ctx, resource); err != nil {
		return err
	}

	if err := r.initCaser(); err != nil {
		return err
	}

	if err := r.initRequestBody(); err != nil {
		return err
	}

	if err := r.initResponseBodyIfNeeded(); err != nil {
		return err
	}

	if err := r.initResponseType(); err != nil {
		return err
	}

	if err := r.normalizePaths(); err != nil {
		return err
	}
	if err := r.initServiceType(); err != nil {
		return err
	}

	r.initCors(resource)
	r.initCompression(resource)
	r.addPrefixFieldIfNeeded()
	r.indexExcluded()
	if err := r.initTransforms(ctx); err != nil {
		return nil
	}

	r._unmarshallerInterceptors = r.Transforms.FilterByKind(marshal.TransformKindUnmarshal)

	if err := r.InitMarshaller(r.ioConfig(), r.Exclude, r.View.Schema.Type(), r._requestBodyType); err != nil {
		return err
	}

	r.initDebugStyleIfNeeded()
	if r.APIKey != nil {
		r._apiKeys = append(r._apiKeys, r.APIKey)
	}
	if err := r.initAsyncIfNeeded(ctx); err != nil {
		return err
	}
	if r._stateCache == nil {
		r._stateCache = &staterCache{
			index: sync.Map{},
		}
	}
	return nil
}

func (r *Route) initView(ctx context.Context, resource *Resource) error {
	if err := r.View.Init(ctx, resource.Resource); err != nil {
		return err
	}

	r.NamespacedView = view.IndexViews(r.View)
	return nil
}

func (r *Route) IsCacheDisabled(req *http.Request) bool {
	if r.EnableDebug == nil {
		return false
	}
	return (*r.EnableDebug) && (req.Header.Get(httputils.DatlyRequestDisableCacheHeader) != "" || req.Header.Get(strings.ToLower(httputils.DatlyRequestDisableCacheHeader)) != "")
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

func (r *Route) initCardinality() error {
	switch r.Cardinality {
	case state.One, state.Many:
		return nil
	case "":
		r.Cardinality = state.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Cardinality)
	}
}

func (r *Route) ioConfig() common.IOConfig {
	return common.IOConfig{
		OmitEmpty:  r.OmitEmpty,
		CaseFormat: *r._caser,
		Exclude:    common.Exclude(r.Exclude).Index(),
		DateLayout: r.DateFormat,
	}
}

func (r *Route) initCors(resource *Resource) {
	if r.Cors == nil {
		r.Cors = resource.Cors
		return
	}

	r.Cors.inherit(resource.Cors)
}

func (r *Route) initResponseType() (err error) {
	if (r.Style == "" || r.Style == BasicStyle) && r.Field == "" {
		r.Style = BasicStyle
		return nil
	}

	if r.Field == "" {
		switch r.Service {
		case ServiceTypeReader:
			r.Field = "Data"
		default:
			r.Field = "ResponseBody"
		}
	}

	fieldType := r.OutputDataType()

	if len(r.Output.Parameters) == 0 {

		r.Output.Parameters.Append(
			state.NewParameter(r.Field,
				state.NewOutputLocation("Data"),
				state.WithParameterType(fieldType)))

		r.Output.Parameters.Append(
			state.NewParameter("Status",
				state.NewOutputLocation("Status"),
				state.WithParameterTag(`anonymous:"true"`),
				state.WithParameterType(reflect.TypeOf(response.Status{}))))
		if r.View.MetaTemplateEnabled() && r.View.Template.Meta.Kind == view.MetaTypeRecord {
			r.Output.Parameters.Append(
				state.NewParameter(r.View.Template.Meta.Name,
					state.NewOutputLocation("Summary"),
					state.WithParameterType(r.View.Template.Meta.Schema.Type())))
		}

		if r.IsRevealMetric() && r.DebugKind == view.MetaTypeRecord {
			r.Output.Parameters.Append(
				state.NewParameter("Debug",
					state.NewOutputLocation("Stats"),
					state.WithParameterType(r.View.Template.Meta.Schema.Type())))
		}

	}

	pkg := r.PgkPath(r.Field)

	switch r.Service {
	case ServiceTypeReader:
		parameters := r.Output.Parameters
		outputType, err := parameters.ReflectType(pkg, r._resource.LookupType(), false)
		if err != nil {
			return err
		}
		r.Output.Type = structology.NewStateType(outputType)
	default:

		//return nil
	}

	responseFields := make([]reflect.StructField, 2)
	responseFields[0] = reflect.StructField{
		Name:      "Status",
		Type:      reflect.TypeOf(response.Status{}),
		Anonymous: true,
	}

	responseFieldPgkPath := r.PgkPath(r.Field)

	responseFields[1] = reflect.StructField{
		Name:    r.Field,
		PkgPath: responseFieldPgkPath,
		Type:    fieldType,
	}

	var metaFieldName string
	if r.View.MetaTemplateEnabled() && r.View.Template.Meta.Kind == view.MetaTypeRecord {
		responseFields = append(responseFields, reflect.StructField{
			Name:    r.View.Template.Meta.Name,
			Type:    r.View.Template.Meta.Schema.Type(),
			PkgPath: r.PgkPath(r.View.Template.Meta.Name),
		})
		metaFieldName = r.View.Template.Meta.Name
	}

	if r.IsRevealMetric() && r.DebugKind == view.MetaTypeRecord {
		responseFields = append(responseFields, reflect.StructField{
			Name: "DatlyDebug",
			Tag:  `json:"_datly_debug_,omitempty"`,
			Type: reflect.TypeOf([]*reader.Info{}),
		})
	}

	responseType := reflect.StructOf(responseFields)
	r._responseSetter = &responseSetter{
		statusField: FieldByName(responseType, "Status"),
		bodyField:   FieldByName(responseType, r.Field),
		metaField:   FieldByName(responseType, metaFieldName),
		infoField:   FieldByName(responseType, "DatlyDebug"),
		rType:       responseType,
	}

	return nil
}

func FieldByName(responseType reflect.Type, name string) *xunsafe.Field {
	if name == "" {
		return nil
	}
	return xunsafe.FieldByName(responseType, name)
}

func (r *Route) PgkPath(fieldName string) string {
	var responseFieldPgkPath string
	if fieldName[0] < 'A' || fieldName[0] > 'Z' {
		responseFieldPgkPath = pkgPath
	}
	return responseFieldPgkPath
}

func (r *Route) OutputDataType() reflect.Type {
	if r.ResponseBody != nil && r.ResponseBody._bodyType != nil {
		return r.ResponseBody._bodyType
	}
	if r.Cardinality == state.Many {
		return r.View.Schema.SliceType()
	}
	return r.View.Schema.Type()
}

func (r *Route) responseType() reflect.Type {
	if r._responseSetter != nil {
		return r._responseSetter.rType
	}

	if r.ResponseBody != nil {
		return r.ResponseBody._bodyType
	}

	return r.View.Schema.Type()
}

func (r *Route) initServiceType() error {
	switch r.Service {
	case "", ServiceTypeReader:
		r.Service = ServiceTypeReader
		return nil
	case ServiceTypeExecutor:
		return nil
	}

	switch r.Method {
	case http.MethodGet:
		r.Service = ServiceTypeReader
		return nil
	default:
		return fmt.Errorf("http method %v unsupported, no default service specified for given method", r.Method)
	}
}

func (r *Route) initRequestBody() error {
	if r.Method == http.MethodGet {
		return nil
	}

	return r.initRequestBodyFromParams()
}

func (r *Route) initRequestBodyFromParams() error {

	params := make([]*state.Parameter, 0)

	//TODO why do we need this ?
	setMarker := map[string]bool{}
	r.findRequestBodyParams(r.View, &params, setMarker)

	if len(params) == 0 {
		return nil
	}
	bodyParam, _ := r.fullBodyParam(params)
	rType, err := r.initRequestBodyType(bodyParam, params)
	if err != nil {
		return err
	}

	r.RequestBodySchema = state.NewSchema(rType)
	r._requestBodyType = rType
	for _, param := range params {
		r._requestBodyParamRequired = r._requestBodyParamRequired || param.IsRequired()
	}

	r._requestBodySlice = xunsafe.NewSlice(reflect.SliceOf(r._requestBodyType))

	return nil
}

func (r *Route) initRequestBodyType(bodyParam *state.Parameter, params []*state.Parameter) (reflect.Type, error) {
	if bodyParam != nil {
		bodyType := bodyParam.Schema.Type()
		return bodyType, r.bodyParamMatches(bodyType, params)
	}

	if r.RequestBodySchema != nil {

		if err := r.RequestBodySchema.Init(view.NewResourcelet(r._resource, nil)); err != nil {
			return nil, err
		}

		return r.RequestBodySchema.Type(), nil
	}

	typeBuilder := template.NewBuilder("")
	for _, param := range params {
		name := param.In.Name
		schemaType := param.Schema.Type()
		if err := typeBuilder.AddType(name, schemaType, reflect.StructTag(param.Tag)); err != nil {
			return nil, err
		}
	}

	return typeBuilder.Build(), nil
}

func (r *Route) findRequestBodyParams(aView *view.View, params *[]*state.Parameter, setMarker map[string]bool) {
	for i, param := range aView.Template.Parameters {
		if param.In.Kind == state.KindRequestBody && !setMarker[param.Name] {
			setMarker[param.Name] = true
			*params = append(*params, aView.Template.Parameters[i])
		}

		//r.findRequestBodyParams(aView, params, setMarker)
	}

	for _, relation := range aView.With {
		r.findRequestBodyParams(&relation.Of.View, params, setMarker)
	}
}

func (r *Route) initCompression(resource *Resource) {
	if r.Compression != nil {
		return
	}

	r.Compression = resource.Compression
}

func (r *Route) ShouldNormalizeExclude() bool {
	return r.NormalizeExclude == nil || *r.NormalizeExclude
}

func (r *Route) normalizePaths() error {
	if !r.ShouldNormalizeExclude() {
		return nil
	}

	if err := r.initCaser(); err != nil {
		return err
	}

	aBool := false
	r.NormalizeExclude = &aBool

	for i, excluded := range r.Exclude {
		r.Exclude[i] = formatter.NormalizePath(excluded)
	}

	for i, transform := range r.Transforms {
		r.Transforms[i].Path = formatter.NormalizePath(transform.Path)
	}

	return nil
}

//func (r *Route) PrefixByView(aView *view.View) (string, bool) {
//	return r.Index.prefixByView(aView)
//}

func (r *Route) indexExcluded() {
	r._excluded = map[string]bool{}
	for _, excluded := range r.Exclude {
		r._excluded[excluded] = true
	}
}

func (r *Route) initCaser() error {
	if r._caser != nil {
		return nil
	}

	if r.CaseFormat == "" {
		r.CaseFormat = formatter.UpperCamel
	}

	var err error
	caser, err := r.CaseFormat.Caser()
	if err != nil {
		return err
	}

	r._caser = &caser

	return nil
}

func (r *Route) fullBodyParam(params []*state.Parameter) (*state.Parameter, bool) {
	for _, param := range params {
		if param.In.Name == "" {
			return param, true
		}
	}

	return nil, false
}

func (r *Route) bodyParamMatches(rType reflect.Type, params []*state.Parameter) error {
	for _, param := range params {
		name := param.In.Name
		if name == "" {
			continue
		}

	}

	return nil
}

func (r *Route) addPrefixFieldIfNeeded() {
	if r.Field == "" {
		return
	}
	for i, actual := range r.Exclude {
		r.Exclude[i] = r.Field + "." + actual
	}
}

func (r *Route) initDebugStyleIfNeeded() {
	if r.RevealMetric == nil || !*r.RevealMetric {
		return
	}

	if r.DebugKind != view.MetaTypeRecord {
		r.DebugKind = view.MetaTypeHeader
	}
}

func (r *Route) initResponseBodyIfNeeded() error {
	if r.ResponseBody == nil {
		return nil
	}

	return r.ResponseBody.Init(r.View)
}

func (r *Route) AddApiKeys(keys ...*APIKey) {
	r._apiKeys = append(r._apiKeys, keys...)
}

func (r *Route) initMarshallerInterceptor() error {
	r._unmarshallerInterceptors = r.Transforms.FilterByKind(marshal.TransformKindUnmarshal)
	return nil
}

func (r *Route) transformFn(request *http.Request, transform *marshal.Transform) func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	unmarshaller := transform.UnmarshalerInto()
	if unmarshaller != nil {
		return unmarshaller.UnmarshalJSONWithOptions
	}
	return func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
		evaluate, err := transform.Evaluate(request, decoder, r._resource.LookupType())
		if err != nil {
			return err
		}
		if evaluate.Ctx.Decoder.Decoded != nil {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(evaluate.Ctx.Decoder.Decoded))
		}
		return nil
	}
}

func (r *Route) initTransforms(ctx context.Context) error {
	for _, transform := range r.Transforms {
		if err := transform.Init(ctx, afs.New(), r._resource.LookupType()); err != nil {
			return err
		}
	}

	return nil
}

func (r *Route) initAsyncIfNeeded(ctx context.Context) error {
	r._async = async.NewChecker()
	if r.Async != nil {
		if err := r.Async.Init(ctx, r._resource, r.View); err != nil {
			return err
		}

		return r.ensureJobTable(ctx)
	}

	return nil
}

func (r *Route) ensureJobTable(ctx context.Context) error {
	_, err := r._async.EnsureTable(ctx, r.Async.Connector, &async.TableConfig{
		RecordType:     reflect.TypeOf(async2.Job{}),
		TableName:      view.AsyncJobsTable,
		Dataset:        r.Async.Dataset,
		CreateIfNeeded: true,
		GenerateAutoPk: false,
	})
	return err
}

func (r *Route) match(ctx context.Context, route *http2.Route) (*Route, error) {
	if r._routeMatcher == nil {
		return nil, fmt.Errorf("route matcher was empty")
	}

	return r._routeMatcher(route)
}

func (r *Route) SetRouteLookup(lookup func(route *http2.Route) (*Route, error)) {
	r._routeMatcher = lookup
}
