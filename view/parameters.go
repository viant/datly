package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
	"unsafe"
)

type (
	//Parameter describes parameters used by the Criteria to filter the View.
	Parameter struct {
		shared.Reference
		Fields     Parameters
		Group      []*Parameter `json:",omitempty"`
		Predicates []*config.PredicateConfig
		Name       string `json:",omitempty"`

		In                *Location   `json:",omitempty"`
		Required          *bool       `json:",omitempty"`
		Description       string      `json:",omitempty"`
		DataType          string      `json:",omitempty"`
		Style             string      `json:",omitempty"`
		MaxAllowedRecords *int        `json:",omitempty"`
		MinAllowedRecords *int        `json:",omitempty"`
		ExpectedReturned  *int        `json:",omitempty"`
		Schema            *Schema     `json:",omitempty"`
		Output            *Codec      `json:",omitempty"`
		Const             interface{} `json:",omitempty"`
		DateFormat        string      `json:",omitempty"`
		ErrorStatusCode   int         `json:",omitempty"`
		Tag               string      `json:",omitempty"`

		_valueAccessor    *types.Accessor
		_presenceAccessor *types.Accessor
		_initialized      bool
		_literalValue     interface{}
		_dependsOn        *Parameter
		_state            *structology.StateType
	}

	ParameterOption func(p *Parameter)

	//Location tells how to get parameter value.
	Location struct {
		Kind Kind   `json:",omitempty"`
		Name string `json:",omitempty"`
	}

	Codec struct {
		shared.Reference
		Name       string   `json:",omitempty"`
		Body       string   `json:",omitempty"`
		Args       []string `json:",omitempty"`
		Schema     *Schema  `json:",omitempty"`
		OutputType string   `json:",omitempty"`

		_initialized bool
		_codec       codec.Instance
	}
)

func (p *Parameter) OutputSchema() *Schema {
	if p.Output != nil && p.Output.Schema != nil {
		return p.Output.Schema
	}
	return p.Schema
}

func (v *Codec) Init(resource Resourcelet, inputType reflect.Type) error {
	if v._initialized {
		return nil
	}

	v._initialized = true

	if err := v.inheritCodecIfNeeded(resource, inputType); err != nil {
		return err
	}

	v.ensureSchema(inputType)

	if err := v.Schema.Init(resource, format.CaseUpperCamel); err != nil {
		return err
	}

	return v.initFnIfNeeded(resource, inputType)
}

func (v *Codec) initFnIfNeeded(resource Resourcelet, inputType reflect.Type) error {
	if v._codec != nil {
		return nil
	}

	fn, err := v.extractCodecFn(resource, inputType)
	if err != nil {
		return err
	}

	v._codec = fn
	resultType, err := fn.ResultType(inputType)
	if err != nil {
		return err
	}

	v.Schema = NewSchema(resultType)
	return nil
}

func (v *Codec) inheritCodecIfNeeded(resource Resourcelet, inputType reflect.Type) error {
	if v.Ref == "" {
		return nil
	}

	if err := v.initSchemaIfNeeded(resource); err != nil {
		return err
	}

	aCodec, err := resource.NamedCodecs().Lookup(v.Ref)
	if err != nil {
		return fmt.Errorf("not found codec with name %v", v.Ref)
	}

	instance, err := v.codecInstance(resource, inputType, aCodec)
	if err != nil {
		return err
	}

	codecType, err := instance.ResultType(inputType)
	if err != nil {
		return err
	}

	v._codec = instance
	v.Schema = NewSchema(codecType)
	return nil
}

func (v *Codec) newCodecInstance(resource Resourcelet, inputType reflect.Type, factory codec.Factory) (codec.Instance, error) {
	opts := []interface{}{resource.LookupType()}
	if columns := resource.IndexedColumns(); len(columns) > 0 {
		opts = append(opts, columns)
	}

	aCodec, err := factory.New(&codec.Config{
		Body:       v.Body,
		ParamType:  inputType,
		Args:       v.Args,
		OutputType: v.OutputType,
	}, opts...)

	if err != nil {
		return nil, err
	}

	return aCodec, nil
}

func (v *Codec) ensureSchema(paramType reflect.Type) {
	if v.Schema == nil {
		v.Schema = &Schema{}
		v.Schema.SetType(paramType)
	}
}

func (v *Codec) extractCodecFn(resource Resourcelet, inputType reflect.Type) (codec.Instance, error) {
	foundCodec, err := resource.NamedCodecs().Lookup(v.Name)
	if err != nil {
		return nil, err
	}

	return v.codecInstance(resource, inputType, foundCodec)
}

func (v *Codec) codecInstance(resource Resourcelet, inputType reflect.Type, foundCodec *codec.Codec) (codec.Instance, error) {
	if foundCodec.Factory != nil {
		return v.newCodecInstance(resource, inputType, foundCodec.Factory)
	}

	return foundCodec.Instance, nil
}

func (v *Codec) Transform(ctx context.Context, value interface{}, options ...interface{}) (interface{}, error) {
	return v._codec.Value(ctx, value, options...)
}

func (v *Codec) initSchemaIfNeeded(resource Resourcelet) error {
	if v.Schema == nil || v.Schema.Type() != nil {
		return nil
	}
	return v.Schema.setType(resource.LookupType(), false)
}

// Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, resource Resourcelet) error {
	if p._initialized == true {
		return nil
	}
	p._initialized = true
	//if p.Codec != nil {
	//	p.Output = p.Codec
	//	p.Codec = nil
	//}
	if err := p.inheritParamIfNeeded(ctx, resource); err != nil {
		return err
	}

	if err := p.initGroupParams(ctx, resource); err != nil {
		return err
	}

	if p.In == nil {
		return fmt.Errorf("parameter %v In can't be empty", p.Name)
	}

	p.In.Kind = Kind(strings.ToLower(string(p.In.Kind)))

	if p.In.Kind == KindLiteral && p.Const == nil {
		return fmt.Errorf("param %v value was not set", p.Name)
	}

	if p.In.Kind == KindDataView {
		if err := p.initDataViewParameter(ctx, resource); err != nil {
			return err
		}
	}

	if p.In.Kind == KindParam {
		if err := p.initParamBasedParameter(ctx, resource); err != nil {
			return err
		}
	}

	if err := p.initSchema(resource); err != nil {
		return err
	}

	if err := p.initCodec(resource); err != nil {
		return err
	}

	return p.Validate()
}

func (p *Parameter) initDataViewParameter(ctx context.Context, resource Resourcelet) error {
	if p.Schema != nil && p.Schema.Type() != nil {
		return nil
	}
	schema, err := resource.ViewSchema(ctx, p.In.Name)
	if err != nil {
		return fmt.Errorf("failed to init view parameter %v, %w", p.Name, err)
	}

	cardinality := Cardinality("")
	if p.Schema != nil {
		cardinality = p.Schema.Cardinality
	}
	p.Schema = schema.copy()

	if cardinality != "" {
		p.Schema.Cardinality = cardinality

	}
	p.Schema.SetType(schema.Type())
	return nil
}

func (p *Parameter) inheritParamIfNeeded(ctx context.Context, resource Resourcelet) error {
	if p.Ref == "" {
		return nil
	}

	param, err := resource.LookupParameter(p.Ref)
	if err != nil {
		return err
	}

	if err = param.Init(ctx, resource); err != nil {
		return err
	}

	p.inherit(param)
	return nil
}

func (p *Parameter) inherit(param *Parameter) {
	p.Name = shared.FirstNotEmpty(p.Name, param.Name)
	p.Description = shared.FirstNotEmpty(p.Description, param.Description)
	p.Style = shared.FirstNotEmpty(p.Style, param.Style)
	p.Tag = shared.FirstNotEmpty(p.Tag, param.Tag)
	if p.Const == nil {
		p.Const = param.Const
	}

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil && param.Schema != nil {
		p.Schema = param.Schema.copy()
	}

	if p.Output == nil {
		p.Output = param.Output
	}

	if p.ErrorStatusCode == 0 {
		p.ErrorStatusCode = param.ErrorStatusCode
	}

	if p.Predicates == nil {
		p.Predicates = param.Predicates
	}

	if len(p.Group) == 0 {
		p.Group = param.Group
	}
}

// Validate checks if parameter is valid
func (p *Parameter) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("parameter name can't be empty")
	}

	if p.In == nil {
		return fmt.Errorf("parameter location can't be empty")
	}

	if err := p.In.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate checks if Location is valid
func (l *Location) Validate() error {
	if err := l.Kind.Validate(); err != nil {
		return err
	}

	if err := ParamName(l.Name).Validate(l.Kind); err != nil {
		return fmt.Errorf("unsupported param name %w", err)
	}

	return nil
}

func (p *Parameter) IsRequired() bool {
	return p.Required != nil && *p.Required == true
}

func (p *Parameter) initSchema(resource Resourcelet) error {
	if p.In.Kind == KindGroup {
		rType, err := BuildTypeWithPresence(p.Group)
		if err != nil {
			return err
		}

		p.Schema = NewSchema(rType)
		p._state = structology.NewStateType(p.Schema.Type())
		p._state.NewState()
		return nil
	}

	if p.In.Kind == KindRequest {
		p.Schema = NewSchema(reflect.TypeOf(&http.Request{}))
		return nil
	}

	if p.Schema == nil {
		if p.In.Kind == KindLiteral {
			p.Schema = NewSchema(reflect.TypeOf(p.Const))
		} else if p.In.Kind == KindRequest {
			p.Schema = NewSchema(reflect.TypeOf(&http.Request{}))
		} else {
			return fmt.Errorf("parameter %v schema can't be empty", p.Name)
		}
	}

	if p.Schema.Type() != nil {
		return nil
	}

	if p.In.Kind == KindLiteral {
		p.Schema = NewSchema(reflect.TypeOf(p.Const))
		return nil
	}

	if p.Schema == nil {
		if p.DataType != "" {
			p.Schema = &Schema{DataType: p.DataType}
		} else {
			return fmt.Errorf("parameter %v schema can't be empty", p.Name)
		}
	}

	if p.Schema.DataType == "" && p.Schema.Name == "" {
		return fmt.Errorf("parameter %v either schema Type or Name has to be specified", p.Name)
	}

	schemaType := shared.FirstNotEmpty(p.Schema.Name, p.Schema.DataType)
	if p.MaxAllowedRecords != nil && *p.MaxAllowedRecords > 1 {
		p.Schema.Cardinality = Many
	}

	if schemaType != "" {
		lookup, err := types.LookupType(resource.LookupType(), schemaType)
		if err != nil {
			return err
		}

		p.Schema.SetType(lookup)
		return nil

	}

	return p.Schema.Init(resource, 0)
}

func (p *Parameter) initSchemaFromType(structType reflect.Type) error {
	if p.Schema == nil {
		p.Schema = &Schema{}
	}

	segments := strings.Split(p.Name, ".")
	field, err := fieldByTemplateName(structType, segments[0])
	if err != nil {
		return err
	}

	p.Schema.SetType(field.Type)
	return nil
}

func (p *Parameter) UpdatePresence(presencePtr unsafe.Pointer) {
	if presencePtr == nil || p._presenceAccessor == nil {
		return
	}

	p._presenceAccessor.SetBool(presencePtr, true)
}

func (p *Parameter) SetAccessor(accessor *types.Accessor) {
	p._valueAccessor = accessor
}

func (p *Parameter) pathFields(path string, structType reflect.Type) ([]*xunsafe.Field, error) {
	segments := strings.Split(path, ".")
	if len(segments) == 0 {
		return nil, fmt.Errorf("path can't be empty")
	}

	xFields := make([]*xunsafe.Field, len(segments))

	xField, err := fieldByTemplateName(structType, segments[0])
	if err != nil {
		return nil, err
	}

	xFields[0] = xField
	for i := 1; i < len(segments); i++ {
		newField, err := fieldByTemplateName(xFields[i-1].Type, segments[i])
		if err != nil {
			return nil, err
		}
		xFields[i] = newField
	}
	return xFields, nil
}

func (p *Parameter) Value(values interface{}) (interface{}, error) {
	return p._valueAccessor.Value(values)
}

func (p *Parameter) ConvertAndSetCtx(ctx context.Context, selector *Selector, value interface{}) error {
	_, err := p.convertAndSet(ctx, selector, value, false)
	return err
}

func (p *Parameter) convertAndSet(ctx context.Context, selector *Selector, value interface{}, converted bool) (interface{}, error) {
	return p.setOnState(ctx, &selector.Parameters, value, converted, selector)
}

func (p *Parameter) setOnState(ctx context.Context, state *ParamState, value interface{}, converted bool, options ...interface{}) (interface{}, error) {
	paramPtr, presencePtr := asValuesPtr(state)
	value, err := p.setValue(ctx, value, paramPtr, converted, options...)
	if err != nil {
		return nil, err
	}

	if presencePtr != nil {
		p.UpdatePresence(presencePtr)
	}

	return value, nil
}

func (p *Parameter) setValue(ctx context.Context, value interface{}, paramPtr unsafe.Pointer, converted bool, options ...interface{}) (interface{}, error) {
	if p._valueAccessor == nil {
		fmt.Printf("[WARN] setValue(): parameter  %v _valueAccessor was nil", p.Name)
		return value, nil
	}
	aCodec := p.Output
	if converted {
		aCodec = nil
	}

	var codecFn codec.Instance
	if aCodec != nil {
		codecFn = aCodec._codec
	}

	if codecFn != nil {
		convertedValue, err := codecFn.Value(ctx, value, options...)
		if err != nil {
			return nil, err
		}
		p._valueAccessor.SetValue(paramPtr, convertedValue)
		return convertedValue, nil
	}

	return p._valueAccessor.SetConvertedAndGet(paramPtr, value, p.DateFormat)
}

func (p *Parameter) Set(selector *Selector, value interface{}) error {
	return p.SetCtx(context.Background(), selector, value)
}

func (p *Parameter) SetCtx(ctx context.Context, selector *Selector, value interface{}) error {
	_, err := p.convertAndSet(ctx, selector, value, true)
	return err
}

func (p *Parameter) UpdateParamState(ctx context.Context, paramState *ParamState, value interface{}, options ...interface{}) error {
	_, err := p.setOnState(ctx, paramState, value, true, options...)
	return err
}

func (p *Parameter) SetAndGet(selector *Selector, value interface{}) (interface{}, error) {
	return p.convertAndSet(context.Background(), selector, value, true)
}

func asValuesPtr(state *ParamState) (paramPtr unsafe.Pointer, presencePtr unsafe.Pointer) {
	if state.Values != nil {
		paramPtr = xunsafe.AsPointer(state.Values)
	}

	if state.Has != nil {
		presencePtr = xunsafe.AsPointer(state.Has)
	}

	return paramPtr, presencePtr
}

func (p *Parameter) SetPresenceField(structType reflect.Type) error {
	fields, err := p.pathFields(p.Name, structType)
	if err != nil {
		return err
	}

	p._presenceAccessor = types.NewAccessor(fields...)

	return nil
}

func (p *Parameter) initCodec(resource Resourcelet) error {
	if p.Output == nil {
		return nil
	}

	if err := p.Output.Init(resource, p.Schema.Type()); err != nil {
		return err
	}

	return nil
}

func (p *Parameter) ActualParamType() reflect.Type {
	if p.Output != nil && p.Output.Schema != nil {
		return p.Output.Schema.Type()
	}

	return p.Schema.Type()
}

func (p *Parameter) UpdateValue(params interface{}, presenceMap interface{}) error {
	if p.Const == nil {
		return nil
	}

	paramsPtr := xunsafe.AsPointer(params)
	presenceMapPtr := xunsafe.AsPointer(presenceMap)

	if _, err := p.setValue(context.Background(), p.Const, paramsPtr, true); err != nil {
		return err
	}

	p.UpdatePresence(presenceMapPtr)
	return nil
}

func (p *Parameter) initParamBasedParameter(ctx context.Context, resource Resourcelet) error {
	param, err := resource.LookupParameter(p.In.Name)
	if err != nil {
		return err
	}

	if err = param.Init(ctx, resource); err != nil {
		return err
	}
	p.Schema = param.Schema.copy()
	p._dependsOn = param
	return nil
}

func (p *Parameter) Parent() *Parameter {
	return p._dependsOn
}

func (p *Parameter) WithAccessors(value, presence *types.Accessor) *Parameter {
	result := *p
	result._valueAccessor = value
	result._presenceAccessor = presence
	return &result
}

func (p *Parameter) ValueAccessor() *types.Accessor {
	return p._valueAccessor
}

func (p *Parameter) PresenceAccessor() *types.Accessor {
	return p._presenceAccessor
}

func (p *Parameter) accessValue(state interface{}, ptr unsafe.Pointer) (interface{}, error) {
	return p._valueAccessor.Value(ptr)
}

func (p *Parameter) accessHas(has interface{}, ptr unsafe.Pointer) (bool, error) {
	value, err := p._presenceAccessor.Value(ptr)
	if err != nil {
		return false, err
	}

	asBool, ok := value.(bool)

	return asBool && ok, nil
}

func (p *Parameter) initGroupParams(ctx context.Context, resource Resourcelet) error {
	for _, parameter := range p.Group {
		if err := parameter.Init(ctx, resource); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parameter) NewState(value interface{}) *structology.State {
	return p._state.WithValue(value)
}

// NamedParameters represents Parameter map indexed by Parameter.Name
type NamedParameters map[string]*Parameter

// Parameters represents slice of parameters
type Parameters []*Parameter

func (p Parameters) FilterByKind(kind Kind) Parameters {
	var result = Parameters{}
	for i, candidate := range p {
		if candidate.In.Kind == kind {
			result = append(result, p[i])
		}
	}
	return result
}

func (s Parameters) SetLiterals(state *structology.State) (err error) {
	for _, parameter := range s.FilterByKind(KindLiteral) {
		if err = state.Set(parameter.Name, parameter.Const); err != nil {
			return err
		}
	}
	return nil
}

func (p Parameters) InitRepeated(state *structology.State) (err error) {
	for _, parameter := range p {
		parameterType := parameter.ActualParamType()
		if parameterType == nil || parameterType.Kind() != reflect.Slice {
			continue
		}
		aSlice := reflect.MakeSlice(parameter.ActualParamType(), 1, 1).Interface()
		if err = state.SetValue(parameter.Name, aSlice); err != nil {
			return err
		}
	}
	return nil
}

func (s Parameters) ReflectType(pkgPath string, lookupType xreflect.LookupType, withSetMarker bool) (reflect.Type, error) {
	var fields []reflect.StructField
	var setMarkerFields []reflect.StructField

	var err error
	for _, param := range s {
		schema := param.OutputSchema()
		if schema == nil {
			return nil, fmt.Errorf("invalid parameter: %v schema was empty", param.Name)
		}
		if schema.DataType == "" && param.DataType != "" {
			schema.DataType = param.DataType
		}
		rType := schema.Type()
		if rType == nil {
			if rType, err = types.LookupType(lookupType, schema.DataType); err != nil {
				return nil, fmt.Errorf("failed to detect parmater '%v' type for: %v  %w", param.Name, schema.DataType, err)
			}
		}
		param.Schema.Cardinality = schema.Cardinality
		if rType != nil {
			fields = append(fields, reflect.StructField{Name: param.Name, Type: rType, PkgPath: PkgPath(param.Name, pkgPath), Tag: reflect.StructTag(param.Tag)})
			setMarkerFields = append(setMarkerFields, reflect.StructField{Name: param.Name, Type: boolType, PkgPath: PkgPath(param.Name, pkgPath)})
		}
	}
	if withSetMarker && len(fields) > 0 {
		setMarkerType := reflect.StructOf(setMarkerFields)
		fields = append(fields, reflect.StructField{Name: "Has", Type: setMarkerType, PkgPath: PkgPath("Has", pkgPath), Tag: `setMarker:"true" sqlx:"-" diff:"-"  `})
	}
	if len(fields) == 0 {
		return reflect.StructOf([]reflect.StructField{{Name: "Dummy", Type: reflect.TypeOf(true)}}), nil
	}
	baseType := reflect.StructOf(fields)
	return baseType, nil
}

func (p Parameters) Len() int {
	return len(p)
}

func (p Parameters) Less(i, j int) bool {
	if p[j].ErrorStatusCode == p[i].ErrorStatusCode {
		return p[j].IsRequired()
	}

	if p[j].ErrorStatusCode == 401 {
		return false
	}

	if p[j].ErrorStatusCode == 403 {
		return p[i].ErrorStatusCode == 401
	}

	return true
}

func (p Parameters) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Append appends parameter
func (p *Parameters) Append(parameter *Parameter) {
	for _, param := range *p {
		if param.Name == parameter.Name {
			return
		}
	}
	*p = append(*p, parameter)
}

// Lookup returns match parameter or nil
func (p Parameters) Lookup(name string) *Parameter {
	for _, param := range p {
		if param.Name == name {
			return param
		}
	}
	return nil
}

// Index indexes parameters by Parameter.Name
func (p Parameters) Index() NamedParameters {
	result := NamedParameters(make(map[string]*Parameter))
	for i, parameter := range p {
		if _, ok := result[parameter.Name]; ok {
			continue
		}
		result[parameter.Name] = p[i]
	}
	return result
}

// Filter filters Parameters with given Kind and creates Template
func (p Parameters) Filter(kind Kind) NamedParameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p NamedParameters) Merge(with NamedParameters) {
	for s := range with {
		p[s] = with[s]
	}
}

// Lookup returns Parameter with given name
func (p NamedParameters) Lookup(name string) (*Parameter, error) {
	if param, ok := p[name]; ok {
		return param, nil
	}
	return nil, fmt.Errorf("not found parameter %v", name)
}

// Register registers parameter
func (p NamedParameters) Register(parameter *Parameter) error {
	if _, ok := p[parameter.Name]; ok {
		fmt.Printf("[WARN] parameter with %v name already exists in given resource", parameter.Name)
	}

	p[parameter.Name] = parameter
	return nil
}

// NewQueryLocation creates a query location
func NewQueryLocation(name string) *Location {
	return &Location{Name: name, Kind: KindQuery}
}

// NewBodyLocation creates a body location
func NewBodyLocation(name string) *Location {
	return &Location{Name: name, Kind: KindRequestBody}
}

// NewDataViewLocation creates a dataview location
func NewDataViewLocation(name string) *Location {
	return &Location{Name: name, Kind: KindDataView}
}

func NewConstLocation(name string) *Location {
	return &Location{Kind: KindLiteral, Name: name}
}

// NewPathLocation creates a structql
func NewPathLocation(name string) *Location {
	return &Location{Name: name, Kind: KindPath}
}

// WithParameterType returns schema type parameter option
func WithParameterType(t reflect.Type) ParameterOption {
	return func(p *Parameter) {
		switch t.Kind() {
		case reflect.String, reflect.Int, reflect.Float64, reflect.Float32, reflect.Bool:
			p.Schema = &Schema{DataType: t.Kind().String()}
			return
		}
		p.Schema = NewSchema(t)
	}
}

// NewRefParameter creates a new ref parameter
func NewRefParameter(name string) *Parameter {
	return &Parameter{Reference: shared.Reference{Ref: name}}
}

// NewParameter creates a parameter
func NewParameter(name string, in *Location, opts ...ParameterOption) *Parameter {
	ret := &Parameter{Name: name, In: in}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func PkgPath(fieldName string, pkgPath string) (fieldPath string) {

	if fieldName[0] > 'Z' || fieldName[0] < 'A' {
		fieldPath = pkgPath
	}
	return fieldPath
}
