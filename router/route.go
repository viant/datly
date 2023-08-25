package router

import (
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/marshal/tabjson"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/datly/view/template"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/structql"
	"github.com/viant/toolbox/format"
	async2 "github.com/viant/xdatly/handler/async"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xlsy"
	"github.com/viant/xmlify"
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

	XLSContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

	JSONFormat = "json"

	XMLFormat = "xml"

	XLSFormat = "xls"

	CSVFormat      = "csv"
	CSVContentType = "text/csv"

	JSONContentType = "application/json"

	JSONDataFormatTabular = "tabular"
	TabularJSONFormat     = "application/json"

	XMLContentType = "application/xml"
)

type (
	Routes []*Route
	Route  struct {
		Async            *Async             `json:",omitempty" yaml:",omitempty"`
		Name             string             `json:",omitempty" yaml:",omitempty"`
		Visitor          *Fetcher           `json:",omitempty"`
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
		Content
		Output

		bodyParamQuery map[string]*query

		ParamStatusError *int         `json:",omitempty"`
		Compression      *Compression `json:",omitempty"`
		Handler          *Handler     `json:",omitempty"`

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

	query struct {
		*structql.Query
		field *xunsafe.Field
	}

	Fetcher struct {
		shared.Reference
		_fetcher interface{}
	}

	JSON struct {
		_jsonMarshaller           *json.Marshaller
		_unmarshallerInterceptors marshal.Transforms
	}

	XLS struct {
		_xlsMarshaller *xlsy.Marshaller
	}

	Input struct {
		RequestBodySchema *state.Schema
	}

	Content struct {
		Marshaller
		DateFormat  string             `json:",omitempty"`
		CSV         *CSVConfig         `json:",omitempty"`
		XLS         *XLSConfig         `json:",omitempty"`
		XML         *XMLConfig         `json:",omitempty"`
		TabularJSON *TabularJSONConfig `json:",omitempty"`
	}

	Marshaller struct {
		XLS
		JSON
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
		Schema       *state.Schema
		Parameters   state.Parameters

		_caser          *format.Case
		_excluded       map[string]bool
		_responseSetter *responseSetter
	}

	CSVConfig struct {
		Separator         string
		NullValue         string
		_config           *csv.Config
		_inputMarshaller  *csv.Marshaller
		_outputMarshaller *csv.Marshaller
		_unwrapperSlice   *xunsafe.Slice
	}

	XLSConfig struct {
		DefaultStyle string
		SheetName    string
		Styles       map[string]string //name of style, values
	}

	TabularJSONConfig struct {
		FloatPrecision         string
		_config                *tabjson.Config
		_requestBodyMarshaller *tabjson.Marshaller
		_outputMarshaller      *tabjson.Marshaller
		_unwrapperSlice        *xunsafe.Slice
	}

	XMLConfig struct {
		FloatPrecision         string
		_config                *xmlify.Config
		_requestBodyMarshaller *xmlify.Marshaller
		_outputMarshaller      *xmlify.Marshaller
	}

	responseSetter struct {
		statusField *xunsafe.Field
		bodyField   *xunsafe.Field
		metaField   *xunsafe.Field
		infoField   *xunsafe.Field
		debug       *xunsafe.Field
		rType       reflect.Type
	}

	ErrorItem struct {
		Location string
		Field    string
		Value    interface{}
		Message  string
		Check    string
	}

	WarningItem struct {
		Message string
		Reason  string
	}

	ResponseStatus struct {
		Status  string                 `json:",omitempty"`
		Message string                 `json:",omitempty"`
		Errors  interface{}            `json:",omitempty"`
		Warning []*WarningItem         `json:",omitempty"`
		Extras  map[string]interface{} `json:",omitempty" default:"embedded=true"`
	}
)

// OutputFormat returns output foramt
func (r *Route) OutputFormat(query url.Values) string {
	outputFormat := query.Get(FormatQuery)
	if outputFormat == "" {
		outputFormat = r.Output.DataFormat
	}
	if outputFormat == "" {
		outputFormat = JSONFormat
	}
	return outputFormat
}

// ContentType returns content type
func (r *Route) ContentType(query url.Values) string {
	switch strings.ToLower(r.OutputFormat(query)) {
	case XLSFormat, XLSContentType:
		return XLSContentType
	case CSVFormat, CSVContentType:
		return CSVContentType
	case XMLFormat, XMLContentType:
		return XMLContentType
	case JSONDataFormatTabular:
		return TabularJSONFormat
	}
	return JSONContentType
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

func (x *XLSConfig) Options() []xlsy.Option {

	var options []xlsy.Option
	if x == nil {
		return options
	}
	if x.DefaultStyle != "" {
		options = append(options, xlsy.WithDefaultStyle(x.DefaultStyle))
	}
	if x.SheetName != "" {
		options = append(options, xlsy.WithTag(&xlsy.Tag{Name: x.SheetName}))
	}
	if len(x.Styles) > 0 {
		var pairs []string
		for k, v := range x.Styles {
			pairs = append(pairs, k, v)
		}
		options = append(options, xlsy.WithNamedStyles(pairs...))
	}
	return options
}

func (r *Route) Marshaller(request *http.Request) *marshal.Marshaller {
	contentType := request.Header.Get(HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(HeaderContentType)))
	switch contentType {
	case CSVContentType:
		return marshal.NewMarshaller(r._requestBodySlice.Type, r.CSV.Unmarshal)
	}
	jsonPathInterceptor := json.UnmarshalerInterceptors{}
	for i := range r._unmarshallerInterceptors {
		transform := r._unmarshallerInterceptors[i]
		jsonPathInterceptor[transform.Path] = r.transformFn(request, transform)
	}
	return marshal.NewMarshaller(r._requestBodyType, func(bytes []byte, i interface{}) error {
		return r._jsonMarshaller.Unmarshal(bytes, i, jsonPathInterceptor, request)
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

	if err := r.initVisitor(resource); err != nil {
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

	if err := r.initStyle(); err != nil {
		return err
	}

	//if err := r.Index.Init(r.View, r.Field); err != nil {
	//	return err
	//}

	if err := r.normalizePaths(); err != nil {
		return err
	}
	r.addPrefixFieldIfNeeded()

	if err := r.initMarshaller(); err != nil {
		return err
	}

	if err := r.initTransforms(ctx); err != nil {
		return nil
	}

	r._unmarshallerInterceptors = r.Transforms.FilterByKind(marshal.TransformKindUnmarshal)
	if err := r.initServiceType(); err != nil {
		return err
	}

	r.initCors(resource)
	r.initCompression(resource)
	r.indexExcluded()

	if err := r.initCSVIfNeeded(); err != nil {
		return err
	}

	if err := r.initTabJSONIfNeeded(); err != nil {
		return err
	}

	if err := r.initXMLIfNeeded(); err != nil {
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

	return updateViewConfig(ctx, resource.Resource, r.View)
}

func updateViewConfig(ctx context.Context, resource *view.Resource, aView *view.View) error {
	//var err error
	//
	//viewNs, ok := nameToNs[aView.Name]
	//if ok {
	//	aViewCopy := *aView
	//	aViewCopy.Selector, err = aViewCopy.Selector.CloneWithNs(ctx, resource, &aViewCopy, viewNs)
	//	if err != nil {
	//		return err
	//	}
	//
	//	*aView = aViewCopy
	//}
	//
	//for _, relation := range aView.With {
	//	if err = updateViewConfig(ctx, resource, nameToNs, &relation.Of.View); err != nil {
	//		return err
	//	}
	//}
	//
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

func reverse(namespace map[string]string) map[string]string {
	result := map[string]string{}

	for key, value := range namespace {
		result[value] = key
	}

	return result
}

func (r *Route) initVisitor(resource *Resource) error {
	if r.Visitor == nil {
		return nil
	}

	if r.Visitor.Ref != "" {
		refVisitor, err := resource._visitors.Lookup(r.Visitor.Ref)
		if err != nil {
			return err
		}

		r.Visitor._fetcher = refVisitor
	}

	return nil
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

func (r *Route) initStyle() error {

	if (r.Style == "" || r.Style == BasicStyle) && r.Field == "" {
		r.Style = BasicStyle
		return nil
	}

	if r.Field == "" {
		r.Field = "ResponseBody"
	}

	responseFields := make([]reflect.StructField, 2)
	responseFields[0] = reflect.StructField{
		Name:      "ResponseStatus",
		Type:      reflect.TypeOf(ResponseStatus{}),
		Anonymous: true,
	}

	responseFieldPgkPath := r.PgkPath(r.Field)

	fieldType := r.responseFieldType()

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
		statusField: FieldByName(responseType, "ResponseStatus"),
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

func (r *Route) responseFieldType() reflect.Type {
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

//func (i *Index) ViewByPrefix(prefix string) (*view.View, error) {
//	aView, ok := i.viewByPrefix(prefix)
//	if !ok {
//		return nil, fmt.Errorf("not found view with prefix %v", prefix)
//	}
//
//	return aView, nil
//}

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

func (r *Route) initCSVIfNeeded() error {
	r.ensureCSV()
	if len(r.CSV.Separator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.CSV.Separator)
	}
	if r.CSV.NullValue == "" {
		r.CSV.NullValue = "null"
	}
	r.CSV._config = &csv.Config{
		FieldSeparator:  r.CSV.Separator,
		ObjectSeparator: "\n",
		EncloseBy:       `"`,
		EscapeBy:        "\\",
		NullValue:       r.CSV.NullValue,
	}
	schemaType := r.View.Schema.Type()
	if schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}
	var err error
	r.CSV._outputMarshaller, err = csv.NewMarshaller(schemaType, r.CSV._config)
	if err != nil {
		return err
	}
	if r._requestBodyType == nil {
		return nil
	}

	r.CSV._unwrapperSlice = r._requestBodySlice
	r.CSV._inputMarshaller, err = csv.NewMarshaller(r._requestBodyType, nil)
	return err
}

func (r *Route) ensureCSV() {
	if r.CSV != nil {
		return
	}
	r.CSV = &CSVConfig{Separator: ","}
}

func (r *Route) initTabJSONIfNeeded() error {

	if r.Output.DataFormat != JSONDataFormatTabular {
		return nil
	}

	if r.TabularJSON == nil {
		r.TabularJSON = &TabularJSONConfig{}
	}

	if r.TabularJSON._config == nil {
		r.TabularJSON._config = &tabjson.Config{}
	}

	if r.TabularJSON._config.FieldSeparator == "" {
		r.TabularJSON._config.FieldSeparator = ","
	}

	if len(r.TabularJSON._config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.TabularJSON._config.FieldSeparator)
	}

	if r.TabularJSON._config.NullValue == "" {
		r.TabularJSON._config.NullValue = "null"
	}

	if r.TabularJSON.FloatPrecision != "" {
		r.TabularJSON._config.StringifierConfig.StringifierFloat32Config.Precision = r.TabularJSON.FloatPrecision
		r.TabularJSON._config.StringifierConfig.StringifierFloat64Config.Precision = r.TabularJSON.FloatPrecision
	}

	if len(r.Exclude) > 0 {
		r.TabularJSON._config.ExcludedPaths = r.Exclude
	}

	schemaType := r.View.Schema.Type()
	if schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}

	var err error
	r.TabularJSON._outputMarshaller, err = tabjson.NewMarshaller(schemaType, r.TabularJSON._config)
	if err != nil {
		return err
	}

	if r._requestBodyType == nil {
		return nil
	}

	r.TabularJSON._unwrapperSlice = r._requestBodySlice
	r.TabularJSON._requestBodyMarshaller, err = tabjson.NewMarshaller(r._requestBodyType, nil)
	return err
}

func (r *Route) initXMLIfNeeded() error {
	if r.XML == nil {
		r.XML = &XMLConfig{}
	}
	if r.XML._config == nil {
		r.XML._config = getDefaultConfig()
	}

	if r.XML._config.FieldSeparator == "" {
		r.XML._config.FieldSeparator = ","
	}

	if len(r.XML._config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.XML._config.FieldSeparator)
	}

	if r.XML._config.NullValue == "" {
		r.XML._config.NullValue = "\u0000"
	}

	if r.XML.FloatPrecision != "" {
		r.XML._config.StringifierConfig.StringifierFloat32Config.Precision = r.XML.FloatPrecision
		r.XML._config.StringifierConfig.StringifierFloat64Config.Precision = r.XML.FloatPrecision
	}

	if len(r.Exclude) > 0 {
		r.XML._config.ExcludedPaths = r.Exclude
	}

	schemaType := r.View.Schema.Type()
	if schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}

	var err error
	r.XML._outputMarshaller, err = xmlify.NewMarshaller(schemaType, r.XML._config)
	if err != nil {
		return err
	}

	if r._requestBodyType == nil {
		return nil
	}
	r.XML._requestBodyMarshaller, err = xmlify.NewMarshaller(r._requestBodyType, nil)
	return err
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

func (c *CSVConfig) Unmarshal(bytes []byte, i interface{}) error {
	return c._inputMarshaller.Unmarshal(bytes, i)
}

func (c *CSVConfig) unwrapIfNeeded(value interface{}) (interface{}, error) {
	if c._unwrapperSlice == nil || value == nil {
		return value, nil
	}

	ptr := xunsafe.AsPointer(value)
	sliceLen := c._unwrapperSlice.Len(ptr)
	switch sliceLen {
	case 0:
		return nil, nil
	case 1:
		return c._unwrapperSlice.ValuePointerAt(ptr, 0), nil
	default:
		return nil, fmt.Errorf("unexpected number of data, expected 0 or 1 but got %v", sliceLen)
	}
}

func (r *Route) AddApiKeys(keys ...*APIKey) {
	r._apiKeys = append(r._apiKeys, keys...)
}

func (r *Route) initMarshaller() error {
	r._jsonMarshaller = json.New(r.ioConfig())
	r._xlsMarshaller = xlsy.NewMarshaller(r.Content.XLS.Options()...)
	return nil
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

// TODO MFI
func getDefaultConfig() *xmlify.Config {
	return &xmlify.Config{
		Style:                  "regularStyle", // style
		RootTag:                "result",
		HeaderTag:              "columns",
		HeaderRowTag:           "column",
		HeaderRowFieldAttr:     "id",
		HeaderRowFieldTypeAttr: "type",
		DataTag:                "rows",
		DataRowTag:             "r",
		DataRowFieldTag:        "c",
		NewLine:                "\n",
		DataRowFieldTypes: map[string]string{
			"uint":    "lg",
			"uint8":   "lg",
			"uint16":  "lg",
			"uint32":  "lg",
			"uint64":  "lg",
			"int":     "lg",
			"int8":    "lg",
			"int16":   "lg",
			"int32":   "lg",
			"int64":   "lg",
			"*uint":   "lg",
			"*uint8":  "lg",
			"*uint16": "lg",
			"*uint32": "lg",
			"*uint64": "lg",
			"*int":    "lg",
			"*int8":   "lg",
			"*int16":  "lg",
			"*int32":  "lg",
			"*int64":  "lg",
			/////
			"float32": "db",
			"float64": "db",
			/////
			"string":  "string",
			"*string": "string",
			//////
			"time.Time":  "dt",
			"*time.Time": "dt",
		},
		HeaderRowFieldType: map[string]string{
			"uint":    "long",
			"uint8":   "long",
			"uint16":  "long",
			"uint32":  "long",
			"uint64":  "long",
			"int":     "long",
			"int8":    "long",
			"int16":   "long",
			"int32":   "long",
			"int64":   "long",
			"*uint":   "long",
			"*uint8":  "long",
			"*uint16": "long",
			"*uint32": "long",
			"*uint64": "long",
			"*int":    "long",
			"*int8":   "long",
			"*int16":  "long",
			"*int32":  "long",
			"*int64":  "long",
			/////
			"float32": "double",
			"float64": "double",
			/////
			"string":  "string",
			"*string": "string",
			//////
			"time.Time":  "date",
			"*time.Time": "date",
		},
		TabularNullValue: "nil=\"true\"",
		RegularRootTag:   "root",
		RegularRowTag:    "row",
		RegularNullValue: "",
		NullValue:        "\u0000",
	}
}
