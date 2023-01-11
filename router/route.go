package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/parameter"
	"github.com/viant/datly/xdatly"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
)

type Style string
type ServiceType string

const pkgPath = "github.com/viant/datly/router"

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"

	ReaderServiceType   ServiceType = "Reader"
	ExecutorServiceType ServiceType = "Executor"

	CSVQueryFormat = "csv"
	CSVFormat      = "text/csv"
	JSONFormat     = "application/json"
	FormatQuery    = "_format"

	HeaderContentType = "Content-Type"
)

type (
	Routes []*Route
	Route  struct {
		Visitor     *xdatly.Visitor
		URI         string
		APIKey      *APIKey
		Method      string
		Service     ServiceType
		View        *view.View
		Cors        *Cors
		EnableAudit bool
		EnableDebug *bool
		Output
		Index

		ParamStatusError *int
		Cache            *cache.Cache
		Compression      *Compression

		_resource          *view.Resource
		_accessors         *view.Accessors
		_presenceProviders map[reflect.Type]*option.PresenceProvider

		_requestBodyParamRequired bool
		_requestBodyType          reflect.Type
		_requestBodySlice         *xunsafe.Slice
		_inputMarshaller          *json.Marshaller
	}

	Output struct {
		Cardinality       view.Cardinality `json:",omitempty"`
		CaseFormat        view.CaseFormat  `json:",omitempty"`
		OmitEmpty         bool             `json:",omitempty"`
		Style             Style            `json:",omitempty"`
		ResponseField     string           `json:",omitempty"`
		Transforms        marshal.Transforms
		Exclude           []string
		NormalizeExclude  *bool
		DateFormat        string     `json:",omitempty"`
		CSV               *CSVConfig `json:",omitempty"`
		RevealMetric      *bool
		DebugKind         view.MetaKind
		ReturnBody        bool `json:",omitempty"`
		RequestBodySchema *view.Schema
		ResponseBody      *BodySelector

		_caser            *format.Case
		_excluded         map[string]bool
		_outputMarshaller *json.Marshaller
		_responseSetter   *responseSetter
	}

	CSVConfig struct {
		Separator              string
		NullValue              string
		_config                *csv.Config
		_requestBodyMarshaller *csv.Marshaller
		_outputMarshaller      *csv.Marshaller
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

	ResponseStatus struct {
		Status  string      `json:",omitempty"`
		Message interface{} `json:",omitempty"`
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

func (r *Route) HttpMethod() string {
	return r.Method
}

func (r *Route) CorsEnabled() bool {
	return r.Cors != nil
}

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if r.Style == BasicStyle {
		r.ResponseField = ""
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

	if err := r.Index.Init(r.View, r.ResponseField); err != nil {
		return err
	}

	if err := r.normalizePaths(); err != nil {
		return err
	}

	r.addPrefixFieldIfNeeded()

	if err := r.initMarshaller(); err != nil {
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

	r.initDebugStyleIfNeeded()
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
		r.Visitor = &xdatly.Visitor{}
		return nil
	}

	if r.Visitor.Ref != "" {
		refVisitor, err := resource._visitors.Lookup(r.Visitor.Ref)
		if err != nil {
			return err
		}

		r.Visitor.Inherit(refVisitor)
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

func (r *Route) initMarshaller() error {
	marshaller, err := json.New(r.responseType(), r.jsonConfig())

	if err != nil {
		return err
	}

	r._outputMarshaller = marshaller
	return nil
}

func (r *Route) jsonConfig() marshal.Default {
	return marshal.Default{
		OmitEmpty:  r.OmitEmpty,
		CaseFormat: *r._caser,
		Exclude:    marshal.Exclude(r.Exclude).Index(),
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
	if (r.Style == "" || r.Style == BasicStyle) && r.ResponseField == "" {
		r.Style = BasicStyle
		return nil
	}

	if r.ResponseField == "" {
		r.ResponseField = "ResponseBody"
	}

	responseFields := make([]reflect.StructField, 2)
	responseFields[0] = reflect.StructField{
		Name:      "ResponseStatus",
		Type:      reflect.TypeOf(ResponseStatus{}),
		Anonymous: true,
	}

	responseFieldPgkPath := r.PgkPath(r.ResponseField)

	fieldType := r.responseFieldType()

	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	responseFields[1] = reflect.StructField{
		Name:    r.ResponseField,
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
		bodyField:   FieldByName(responseType, r.ResponseField),
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
	case "", ReaderServiceType:
		r.Service = ReaderServiceType
		return nil
	case ExecutorServiceType:
		return nil
	}

	switch r.Method {
	case http.MethodGet:
		r.Service = ReaderServiceType
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
	r.findRequestBodyParams(r.View, &params)

	if len(params) == 0 {
		return nil
	}

	accessors := view.NewAccessors()
	r._accessors = accessors
	bodyParam, _ := r.fullBodyParam(params)
	rType, err := r.initRequestBodyType(bodyParam, params)
	if err != nil {
		return err
	}

	r._requestBodyType = rType
	r._inputMarshaller, err = json.New(rType, r.jsonConfig())
	if err != nil {
		return err
	}

	r._accessors.Init(r._requestBodyType)
	for _, param := range params {
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
		if err := r.RequestBodySchema.Init(nil, nil, *r.Output._caser, r._resource.GetTypes(), nil); err != nil {
			return nil, err
		}

		return r.RequestBodySchema.Type(), nil
	}

	typeBuilder := parameter.NewBuilder("")
	for _, param := range params {
		name := param.In.Name
		schemaType := param.Schema.Type()
		if err := typeBuilder.AddType(name, schemaType); err != nil {
			return nil, err
		}
	}

	return typeBuilder.Build(), nil
}

func (r *Route) findRequestBodyParams(aView *view.View, params *[]*view.Parameter) {
	for i, param := range aView.Template.Parameters {
		if param.In.Kind == view.KindRequestBody {
			*params = append(*params, aView.Template.Parameters[i])
		}

		if param.View() != nil {
			r.findRequestBodyParams(param.View(), params)
		}
	}

	for _, relation := range aView.With {
		r.findRequestBodyParams(&relation.Of.View, params)
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
		r.CaseFormat = view.UpperCamel
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
	if r.ResponseField == "" {
		return
	}

	for i, actual := range r.Exclude {
		r.Exclude[i] = r.ResponseField + "." + actual
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
