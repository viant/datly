package router

import (
	"context"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/marshal/tabjson"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/parameter"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/structql"
	"github.com/viant/toolbox/format"
	"github.com/viant/xlsy"
	"github.com/viant/xmlify"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
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

		JSON
		XLS
		Output
		Index
		bodyParamQuery   map[string]*query
		ParamStatusError *int         `json:",omitempty"`
		Cache            *cache.Cache `json:",omitempty"`
		Compression      *Compression `json:",omitempty"`
		Handler          *Handler     `json:",omitempty"`

		_resource  *view.Resource
		_accessors *types.Accessors

		_requestBodyParamRequired bool
		_requestBodyType          reflect.Type
		_requestBodySlice         *xunsafe.Slice
		_apiKeys                  []*APIKey
		_stateCache               *staterCache
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
		_unmarshallerInterceptors []*jsonUnmarshallerInterceptors
	}

	XLS struct {
		_xlsMarshaller *xlsy.Marshaller
	}
	jsonUnmarshallerInterceptors struct {
		transform *marshal.Transform
	}

	Output struct {
		Cardinality       view.Cardinality     `json:",omitempty"`
		CaseFormat        formatter.CaseFormat `json:",omitempty"`
		OmitEmpty         bool                 `json:",omitempty"`
		Style             Style                `json:",omitempty"`
		Field             string               `json:",omitempty"`
		Exclude           []string
		NormalizeExclude  *bool
		DateFormat        string             `json:",omitempty"`
		CSV               *CSVConfig         `json:",omitempty"`
		XLS               *XLSConfig         `json:",omitempty"`
		XML               *XMLConfig         `json:",omitempty"`
		TabularJSON       *TabularJSONConfig `json:",omitempty"`
		RevealMetric      *bool
		DebugKind         view.MetaKind
		RequestBodySchema *view.Schema
		ResponseBody      *BodySelector
		DataFormat        string `json:",omitempty"`

		_caser          *format.Case
		_excluded       map[string]bool
		_responseSetter *responseSetter
	}

	CSVConfig struct {
		Separator              string
		NullValue              string
		_config                *csv.Config
		_requestBodyMarshaller *csv.Marshaller
		_outputMarshaller      *csv.Marshaller
		_unwrapperSlice        *xunsafe.Slice
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
		_unwrapperSlice        *xunsafe.Slice
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

	if err := r.Index.Init(r.View, r.Field); err != nil {
		return err
	}

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

	if err := r.initMarshallerInterceptor(); err != nil {
		return err
	}

	if err := r.initServiceType(); err != nil {
		return err
	}

	if err := r.initCache(ctx); err != nil {
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

	return updateViewConfig(ctx, resource.Resource, reverse(r.Namespace), r.View)
}

func updateViewConfig(ctx context.Context, resource *view.Resource, nameToNs map[string]string, aView *view.View) error {
	var err error

	viewNs, ok := nameToNs[aView.Name]
	if ok {
		aViewCopy := *aView
		aViewCopy.Selector, err = aViewCopy.Selector.CloneWithNs(ctx, resource, &aViewCopy, viewNs)
		if err != nil {
			return err
		}

		*aView = aViewCopy
	}

	for _, relation := range aView.With {
		if err = updateViewConfig(ctx, resource, nameToNs, &relation.Of.View); err != nil {
			return err
		}
	}

	return nil
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
	case view.One, view.Many:
		return nil
	case "":
		r.Cardinality = view.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Cardinality)
	}
}

func (r *Route) jsonConfig() common.DefaultConfig {
	return common.DefaultConfig{
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

	if r.Cardinality == view.Many {
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

	params := make([]*view.Parameter, 0)

	//TODO why do we need this ?
	setMarker := map[string]bool{}
	r.findRequestBodyParams(r.View, &params, setMarker)

	if len(params) == 0 {
		return nil
	}
	r.bodyParamQuery = map[string]*query{}
	accessors := types.NewAccessors(&types.VeltyNamer{})
	r._accessors = accessors
	bodyParam, _ := r.fullBodyParam(params)
	rType, err := r.initRequestBodyType(bodyParam, params)
	if err != nil {
		return err
	}

	r._requestBodyType = rType

	r._accessors.Init(r._requestBodyType)
	for _, param := range params {
		if param.In.Name != "" {
			aQuery := &query{}
			QL := fmt.Sprintf("SELECT %v FROM `/`", param.In.Name)
			if aQuery.Query, err = structql.NewQuery(QL, rType, nil); err != nil {
				return fmt.Errorf("failed build query for param %v in requet type: %s due to: %w", param.In.Name, rType.String(), err)
			}
			if destType := aQuery.StructType(); destType != nil {
				aQuery.field = xunsafe.NewField(destType.Field(0))
			}
			r.bodyParamQuery[param.In.Name] = aQuery
		}
		r._requestBodyParamRequired = r._requestBodyParamRequired || param.IsRequired()
	}

	r._requestBodySlice = xunsafe.NewSlice(reflect.SliceOf(r._requestBodyType))

	return nil
}

func (r *Route) initRequestBodyType(bodyParam *view.Parameter, params []*view.Parameter) (reflect.Type, error) {
	if bodyParam != nil {
		bodyType := bodyParam.Schema.Type()
		r._accessors.Init(bodyType)
		return bodyType, r.bodyParamMatches(bodyType, params)
	}

	if r.RequestBodySchema != nil {

		if err := r.RequestBodySchema.Init(view.NewResourcelet(r._resource, nil), *r.Output._caser); err != nil {
			return nil, err
		}

		return r.RequestBodySchema.Type(), nil
	}

	typeBuilder := parameter.NewBuilder("")
	for _, param := range params {
		name := param.In.Name
		schemaType := param.Schema.Type()
		if err := typeBuilder.AddType(name, schemaType, reflect.StructTag(param.Tag)); err != nil {
			return nil, err
		}
	}

	return typeBuilder.Build(), nil
}

func (r *Route) findRequestBodyParams(aView *view.View, params *[]*view.Parameter, setMarker map[string]bool) {
	for i, param := range aView.Template.Parameters {
		if param.In.Kind == view.KindRequestBody && !setMarker[param.Name] {
			setMarker[param.Name] = true
			*params = append(*params, aView.Template.Parameters[i])
		}

		//r.findRequestBodyParams(aView, params, setMarker)
	}

	for _, relation := range aView.With {
		r.findRequestBodyParams(&relation.Of.View, params, setMarker)
	}
}

func (r *Route) initCache(ctx context.Context) error {
	if r.Cache == nil {
		return nil
	}

	return r.Cache.Init(ctx)
}

func (r *Route) initCompression(resource *Resource) {
	if r.Compression != nil {
		return
	}

	r.Compression = resource.Compression
}

func (i *Index) ViewByPrefix(prefix string) (*view.View, error) {
	aView, ok := i.viewByPrefix(prefix)
	if !ok {
		return nil, fmt.Errorf("not found view with prefix %v", prefix)
	}

	return aView, nil
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
		r.Exclude[i] = NormalizePath(excluded)
	}

	for i, transform := range r.Transforms {
		r.Transforms[i].Path = NormalizePath(transform.Path)
	}

	return nil
}

func (r *Route) PrefixByView(aView *view.View) (string, bool) {
	return r.Index.prefixByView(aView)
}

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

func (r *Route) fullBodyParam(params []*view.Parameter) (*view.Parameter, bool) {
	for _, param := range params {
		if param.In.Name == "" {
			return param, true
		}
	}

	return nil, false
}

func (r *Route) bodyParamMatches(rType reflect.Type, params []*view.Parameter) error {
	for _, param := range params {
		name := param.In.Name
		if name == "" {
			continue
		}

		if _, err := r._accessors.AccessorByName(name); err != nil {
			return err
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
	if r.CSV == nil {
		return nil
	}

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
	r.CSV._requestBodyMarshaller, err = csv.NewMarshaller(r._requestBodyType, nil)
	return err
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

	if r.Output.DataFormat != XMLFormat {
		return nil
	}

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

	r.XML._unwrapperSlice = r._requestBodySlice
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

func (c *CSVConfig) presenceMap() PresenceMapFn {
	return func(bytes []byte) (map[string]interface{}, error) {
		result := map[string]interface{}{}
		fieldNames, err := c._requestBodyMarshaller.ReadHeaders(bytes)
		if err != nil {
			return result, err
		}

		for _, name := range fieldNames {
			result[name] = true
		}

		return result, err
	}
}

func (c *CSVConfig) Unmarshal(bytes []byte, i interface{}) error {
	return c._requestBodyMarshaller.Unmarshal(bytes, i)
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
	r._jsonMarshaller = json.New(r.jsonConfig())
	r._xlsMarshaller = xlsy.NewMarshaller(r.Output.XLS.Options()...)
	return nil
}

func (r *Route) initMarshallerInterceptor() error {
	var outputTransforms []*marshal.Transform
	for _, transform := range r.Transforms {
		if transform.Kind != marshal.TransformKindUnmarshal {
			continue
		}

		outputTransforms = append(outputTransforms, transform)
	}

	r._unmarshallerInterceptors = []*jsonUnmarshallerInterceptors{}
	for _, transform := range outputTransforms {
		r._unmarshallerInterceptors = append(r._unmarshallerInterceptors, &jsonUnmarshallerInterceptors{
			transform: transform,
		})
	}

	return nil
}

func (r *Route) unmarshallerInterceptors(params *RequestParams) json.UnmarshalerInterceptors {
	result := json.UnmarshalerInterceptors{}
	for i := range r._unmarshallerInterceptors {
		transform := r._unmarshallerInterceptors[i].transform
		result[transform.Path] = r.transformFn(params, transform)
	}
	return result
}

func (r *Route) transformFn(params *RequestParams, transform *marshal.Transform) func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	unmarshaller := transform.UnmarshalerInto()
	if unmarshaller != nil {
		return unmarshaller.UnmarshalJSONWithOptions
	}

	return func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
		evaluate, err := transform.Evaluate(params.cookiesIndex, params.pathIndex, params.queryIndex, params.request.Header, decoder, r._resource.LookupType())
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
	if r.Async == nil {
		return nil
	}

	return r.Async.Init(ctx, r._resource, r.View)
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
