package router

import (
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/content"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/service"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	async2 "github.com/viant/xdatly/handler/async"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xunsafe"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

const pkgPath = "github.com/viant/datly/router"

const (
	HeaderContentType = "Content-Type"

	FormatQuery = "_format"
)

type (
	Routes []*Route
	Route  struct {
		Async  *Async  `json:",omitempty" yaml:",omitempty"`
		APIKey *APIKey `json:",omitempty"`

		Component

		CustomValidation bool `json:",omitempty"`

		Cors        *Cors              `json:",omitempty"`
		EnableAudit bool               `json:",omitempty"`
		EnableDebug *bool              `json:",omitempty"`
		Transforms  marshal.Transforms `json:",omitempty"`

		Compression *Compression `json:",omitempty"`

		_unmarshallerInterceptors marshal.Transforms

		_resource     *view.Resource
		_resourcelet  state.Resourcelet
		_apiKeys      []*APIKey
		_routeMatcher func(route *http2.Route) (*Route, error)
		_async        *async.Async
		_router       *Router
	}
)

func (r *Route) OutputType() reflect.Type {

	if r.Output.Type.Schema == nil {
		return nil
	}
	if parameter := r.Output.Type.AnonymousParameters(); parameter != nil {
		return parameter.OutputType()
	}
	return r.Output.Type.Schema.Type()
}

func (r *Route) InputType() reflect.Type {
	if r.Input.Type.Schema == nil {
		return nil
	}
	return r.Input.Type.Schema.Type()
}

func (r *Route) Exclusion(state *view.State) []*json.FilterEntry {
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

// OutputFormat returns output format
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
	if r.Output.RevealMetric == nil {
		return false
	}
	return *r.Output.RevealMetric
}

func (r *Route) HttpURI() string {
	return r.URI
}

func (r *Route) Marshaller(request *http.Request) *marshal.Marshaller {
	contentType := request.Header.Get(HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(HeaderContentType)))
	switch contentType {
	case content.CSVContentType:
		return marshal.NewMarshaller(r.View.Schema.SliceType(), r.CSV.Unmarshal)
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	for i := range r._unmarshallerInterceptors {
		transform := r._unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = r.transformFn(request, transform)
	}

	return marshal.NewMarshaller(r.OutputType(), func(bytes []byte, i interface{}) error {
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
	result = append(result, locator.WithOutputParameters(r.Output.Type.Parameters))
	if r.Input.Type.Schema != nil {
		result = append(result, locator.WithBodyType(r.InputType()))
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
	if r.Output.Style == component.BasicStyle {
		r.Output.Field = ""
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
	r._resourcelet = view.NewResourcelet(resource.Resource, r.View)

	if err := r.initInput(); err != nil {
		return err
	}

	if err := r.initOutput(); err != nil {
		return err
	}

	fmt.Printf("OUTPUT: %s\n", r.OutputType().String())

	if err := r.normalizePaths(); err != nil {
		return err
	}
	if err := r.initServiceType(); err != nil {
		return err
	}

	r.initCors(resource)
	r.initCompression(resource)
	r.addPrefixFieldIfNeeded()
	if err := r.initTransforms(ctx); err != nil {
		return nil
	}

	r._unmarshallerInterceptors = r.Transforms.FilterByKind(marshal.TransformKindUnmarshal)

	if err := r.InitMarshaller(r.ioConfig(), r.Output.Exclude, r.InputType(), r.OutputType()); err != nil {
		return err
	}

	r.initDebugStyleIfNeeded()
	if r.APIKey != nil {
		r._apiKeys = append(r._apiKeys, r.APIKey)
	}
	if err := r.initAsyncIfNeeded(ctx); err != nil {
		return err
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
	switch r.Output.Cardinality {
	case state.One, state.Many:
		return nil
	case "":
		r.Output.Cardinality = state.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Output.Cardinality)
	}
}

func (r *Route) ioConfig() common.IOConfig {
	return common.IOConfig{
		OmitEmpty:  r.Output.OmitEmpty,
		CaseFormat: *r.Output.FormatCase(),
		Exclude:    common.Exclude(r.Output.Exclude).Index(),
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

func (r *Route) initServiceType() error {
	switch r.Service {
	case "", service.TypeReader:
		r.Service = service.TypeReader
		return nil
	case service.TypeExecutor:
		return nil
	}

	switch r.Method {
	case http.MethodGet:
		r.Service = service.TypeReader
		return nil
	default:
		return fmt.Errorf("http method %v unsupported, no default service specified for given method", r.Method)
	}
}

func (r *Route) initInput() error {
	if len(r.Input.Type.Parameters) == 0 {
		r.Input.Type.Parameters = r.View.InputParameters()
	}
	if err := r.Input.Type.Init(state.WithResourcelet(r._resourcelet),
		state.WithPackage(pkgPath),
		state.WithMarker(true),
		state.WithBodyType(true)); err != nil {
		return fmt.Errorf("failed to initialise input: %w", err)
	}
	return nil
}

func (r *Route) initOutput() (err error) {
	if err = r.Output.Init(); err != nil {
		return err
	}

	if err = r.initializeOutputParameters(); err != nil {
		return err
	}
	if (r.Output.Style == "" || r.Output.Style == component.BasicStyle) && r.Output.Field == "" {
		r.Output.Style = component.BasicStyle
		if r.Service == service.TypeReader {
			r.Output.Type.Schema = state.NewSchema(r.View.OutputType())
			return nil
		}
	}

	if r.Output.Field == "" && r.Output.Style != component.BasicStyle {
		switch r.Service {
		case service.TypeReader:
			r.Output.Field = "Data"
		default:
			r.Output.Field = "ResponseBody"
		}
	}
	if err = r.Output.Type.Init(state.WithResourcelet(r._resourcelet), state.WithPackage(pkgPath)); err != nil {
		return fmt.Errorf("failed to initialise output: %w", err)
	}
	return nil
}

func (r *Route) initializeOutputParameters() (err error) {
	if dataParameter := r.Output.Type.Parameters.LookupByLocation(state.KindOutput, "data"); dataParameter != nil {
		r.Output.Style = component.ComprehensiveStyle
		r.Output.Field = dataParameter.Name
	}
	if len(r.Output.Type.Parameters) == 0 {
		r.Output.Type.Parameters, err = r.defaultOutputParameters()
	}
	for _, parameter := range r.Output.Type.Parameters {
		r.initializeOutputParameter(parameter)
	}
	return err
}

func (r *Route) defaultOutputParameters() (state.Parameters, error) {
	var parameters state.Parameters
	if r.Service == service.TypeReader && r.Output.Style == component.ComprehensiveStyle {
		parameters = state.Parameters{
			{Name: r.Output.Field, In: state.NewOutputLocation("data")},
			{Name: "Status", In: state.NewOutputLocation("status"), Tag: `anonymous:"true"`},
		}
		if r.View.MetaTemplateEnabled() && r.View.Template.Meta.Kind == view.MetaTypeRecord {
			parameters = append(parameters, state.NewParameter(r.View.Template.Meta.Name,
				state.NewOutputLocation("Summary"),
				state.WithParameterType(r.View.Template.Meta.Schema.Type())))
		}

		if r.IsRevealMetric() && r.Output.DebugKind == view.MetaTypeRecord {
			parameters = append(parameters,
				state.NewParameter("Debug",
					state.NewOutputLocation("Stats"),
					state.WithParameterType(r.View.Template.Meta.Schema.Type())))
		}

	} else if r.Output.ResponseBody != nil && r.Output.ResponseBody.StateValue != "" {
		inputParameter := r.Input.Type.Parameters.Lookup(r.Output.ResponseBody.StateValue)
		if inputParameter == nil {
			return nil, fmt.Errorf("failed to lookup state value: %s", r.Output.ResponseBody.StateValue)
		}
		name := inputParameter.In.Name
		tag := ""
		if name == "" { //embed
			tag = `anonymous:"true"`
			name = r.Output.ResponseBody.StateValue
		}
		parameters = state.Parameters{
			{Name: name, In: state.NewState(r.Output.ResponseBody.StateValue), Schema: inputParameter.Schema, Tag: tag},
		}
		if inputParameter.In.Name != "" {
			parameters = append(parameters, &state.Parameter{Name: "Status", In: state.NewOutputLocation("status"), Tag: `anonymous:"true"`})
		}
	}
	return parameters, nil
}

func (r *Route) initializeOutputParameter(parameter *state.Parameter) {
	switch parameter.In.Kind {
	case state.KindOutput:
		switch parameter.In.Name {
		case "data":
			parameter.Schema = state.NewSchema(r.View.OutputType())
		case "sql":
			parameter.Schema = state.NewSchema(reflect.TypeOf(""))
		case "status":
			parameter.Schema = state.NewSchema(reflect.TypeOf(response.Status{}))
			if parameter.Tag == "" {
				parameter.Tag = ` anonymous:"true"`
			}
		case "summary":
			parameter.Schema = r.View.Template.Meta.Schema
		case "filter":
			predicateType := r.View.Template.Parameters.PredicateStructType()
			parameter.Schema = state.NewSchema(predicateType)
		}
	}
}

func (r *Route) initCompression(resource *Resource) {
	if r.Compression != nil {
		return
	}

	r.Compression = resource.Compression
}

func (r *Route) normalizePaths() error {
	if !r.Output.ShouldNormalizeExclude() {
		return nil
	}
	for i, transform := range r.Transforms {
		r.Transforms[i].Path = formatter.NormalizePath(transform.Path)
	}

	return nil
}

//func (r *Route) PrefixByView(aView *view.View) (string, bool) {
//	return r.Index.prefixByView(aView)
//}

func (r *Route) addPrefixFieldIfNeeded() {
	if r.Output.Field == "" {
		return
	}
	for i, actual := range r.Output.Exclude {
		r.Output.Exclude[i] = r.Output.Field + "." + actual
	}
}

func (r *Route) initDebugStyleIfNeeded() {
	if r.Output.RevealMetric == nil || !*r.Output.RevealMetric {
		return
	}

	if r.Output.DebugKind != view.MetaTypeRecord {
		r.Output.DebugKind = view.MetaTypeHeader
	}
}

func (r *Route) AddApiKeys(keys ...*APIKey) {
	r._apiKeys = append(r._apiKeys, keys...)
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
		//if err := r.Async.Init(ctx, r._resource, r.View); err != nil {
		//	return err
		//}

		//return r.ensureJobTable(ctx)
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
