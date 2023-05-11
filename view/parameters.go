package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

const (
	CodecVeltyCriteria = "VeltyCriteria"
)

type (
	//Parameter describes parameters used by the Criteria to filter the view.
	Parameter struct {
		shared.Reference
		Name         string `json:",omitempty"`
		PresenceName string `json:",omitempty"`

		In                *Location `json:",omitempty"`
		Required          *bool     `json:",omitempty"`
		Description       string    `json:",omitempty"`
		DataType          string    `json:",omitempty"`
		Style             string    `json:",omitempty"`
		MaxAllowedRecords *int      `json:",omitempty"`
		MinAllowedRecords *int      `json:",omitempty"`
		ExpectedReturned  *int      `json:",omitempty"`
		Schema            *Schema   `json:",omitempty"`
		//Deprecated -> use Codec only to set Output
		Codec           *Codec      `json:",omitempty"`
		Output          *Codec      `json:",omitempty"`
		Const           interface{} `json:",omitempty"`
		DateFormat      string      `json:",omitempty"`
		ErrorStatusCode int         `json:",omitempty"`

		_valueAccessor    *types.Accessor
		_presenceAccessor *types.Accessor
		_initialized      bool
		_view             *View
		_owner            *View
		_literalValue     interface{}
		_dependsOn        *Parameter
	}

	ParameterOption func(p *Parameter)

	//Location tells how to get parameter value.
	Location struct {
		Kind Kind   `json:",omitempty"`
		Name string `json:",omitempty"`
	}

	Codec struct {
		shared.Reference
		Name string `json:",omitempty"`
		config.CodecConfig
		Schema *Schema `json:",omitempty"`

		_initialized bool
		_codecFn     config.CodecFn
	}
)

func (v *Codec) Init(resource *Resource, view *View, ownerType reflect.Type) error {
	if v._initialized {
		return nil
	}

	v._initialized = true

	if err := v.inheritCodecIfNeeded(resource, ownerType); err != nil {
		return err
	}

	v.ensureSchema(ownerType)
	if v.SourceURL != "" && v.Source == "" {
		data, err := resource.LoadText(context.Background(), v.SourceURL)
		if err != nil {
			return err
		}
		v.Source = data
	}

	if err := v.Schema.Init(nil, nil, format.CaseUpperCamel, resource, nil); err != nil {
		return err
	}

	return v.initFnIfNeeded(resource, view)
}

func (v *Codec) initFnIfNeeded(resource *Resource, view *View) error {
	if v._codecFn != nil {
		return nil
	}

	fn, err := v.extractCodecFn(resource, v.Schema.Type(), view)
	if err != nil {
		return err
	}

	v._codecFn = fn
	return nil
}

func (v *Codec) inheritCodecIfNeeded(resource *Resource, paramType reflect.Type) error {
	if v.Ref == "" {
		return nil
	}

	if err := v.initSchemaIfNeeded(resource); err != nil {
		return err
	}

	visitor, ok := resource.CodecByName(v.Ref)
	if !ok {
		return fmt.Errorf("not found codec with name %v", v.Ref)
	}

	factory, ok := visitor.(config.CodecFactory)
	if ok {
		aCodec, err := factory.New(&v.CodecConfig, paramType, xreflect.TypeLookupFn(resource.LookupType))
		if err != nil {
			return err
		}

		v._codecFn = aCodec.Value
		if typeProvider, ok := aCodec.(config.Typer); ok {
			rType, err := typeProvider.ResultType(paramType)
			if err != nil {
				return err
			}

			v.Schema = NewSchema(rType)
		}

		return nil
	}

	asCodec, ok := visitor.(config.CodecDef)
	if !ok {
		return fmt.Errorf("expected visitor to be type of %T but was %T", asCodec, visitor)
	}

	return v.inherit(asCodec, paramType)
}

func (v *Codec) ensureSchema(paramType reflect.Type) {
	if v.Schema == nil {
		v.Schema = &Schema{}
		v.Schema.SetType(paramType)
	}
}

func (v *Codec) extractCodecFn(resource *Resource, paramType reflect.Type, view *View) (config.CodecFn, error) {
	switch strings.ToLower(v.Name) {
	case strings.ToLower(CodecVeltyCriteria):
		veltyCodec, err := NewVeltyCodec(v.Source, paramType, view)
		if err != nil {
			return nil, err
		}
		return veltyCodec.Value, nil
	}

	vVisitor, err := resource._visitors.Lookup(v.Name)
	if err != nil {
		return nil, err
	}

	switch actual := vVisitor.(type) {
	case config.BasicCodec:
		return actual.Valuer().Value, nil
	case config.CodecDef:
		return actual.Valuer().Value, nil
	default:
		return nil, fmt.Errorf("expected %T to implement Codec", actual)
	}
}

func (v *Codec) Transform(ctx context.Context, raw string, options ...interface{}) (interface{}, error) {
	return v._codecFn(ctx, raw, options...)
}

func (v *Codec) inherit(asCodec config.CodecDef, paramType reflect.Type) error {
	v.Name = asCodec.Name()
	resultType, err := asCodec.ResultType(paramType)
	if err != nil {
		return err
	}

	v.Schema = NewSchema(resultType)
	v.Schema.DataType = resultType.String()
	v._codecFn = asCodec.Valuer().Value
	return nil
}

func (v *Codec) initSchemaIfNeeded(resource *Resource) error {
	if v.Schema == nil || v.Schema.Type() != nil {
		return nil
	}

	return v.Schema.parseType(resource._types)
}

//Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, view *View, resource *Resource, structType reflect.Type) error {
	if p._initialized == true {
		return nil
	}
	p._initialized = true
	if p.Codec != nil {
		p.Output = p.Codec
		p.Codec = nil
	}

	p._owner = view

	if err := p.inheritParamIfNeeded(ctx, view, resource, structType); err != nil {
		return err
	}

	if p.PresenceName == "" {
		p.PresenceName = p.Name
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
		if err := p.initParamBasedParameter(ctx, view, resource); err != nil {
			return err
		}
	}

	if err := p.initSchema(resource, structType); err != nil {
		return err
	}

	if err := p.initCodec(resource, view, p.Schema.Type()); err != nil {
		return err
	}

	return p.Validate()
}

//func (p *Parameter) initStructQLParameter(ctx context.Context, view *View, resource *Resource) error {
//
//	if p.Query == "" {
//		return fmt.Errorf("structql param: %v, query is empty", p.Name)
//	}
//	name := p.In.Name
//	var pType reflect.Type
//	if name == "" {
//		pType = view.Schema.Type()
//	} else {
//		refParam, err := view.ParamByName(name)
//		if err != nil {
//			return fmt.Errorf("failed to build structql param: %v, %w", name, err)
//		}
//		if refParam.Schema == nil {
//			_ = refParam.Init(ctx, view, resource, nil)
//		}
//		if refParam.Schema == nil {
//			return fmt.Errorf("failed to build structql param %v, ref param %v schema was nil", name, refParam.Name)
//		}
//		pType = refParam.Schema.Type()
//	}
//
//	query, err := structql.NewQuery(p.Query, pType, nil)
//	if err != nil {
//		return fmt.Errorf("failed to build structql param: %v, %w", name, err)
//	}
//	p.structQL = query
//	p.Schema = NewSchema(query.Type())
//	return nil
//}

func (p *Parameter) initDataViewParameter(ctx context.Context, resource *Resource) error {
	aView, err := resource.View(p.In.Name)
	if err != nil {
		return fmt.Errorf("failed to lookup parameter %v view %w", p.Name, err)
	}

	if err = aView.Init(ctx, resource); err != nil {
		return err
	}

	p._view = aView
	if p.Schema == nil {
		p.Schema = NewSchema(aView.Schema.SliceType())
	} else {
		p.Schema.DataType = aView.Schema.DataType
		p.Schema.Name = aView.Schema.Name

		if FirstNotEmpty(p.Schema.DataType, p.Schema.Name) == "" {
			elemType := aView.Schema.Type()
			if elemType.Kind() == reflect.Slice {
				elemType = elemType.Elem()
			}

			p.Schema.SetType(elemType)
		}
	}
	return nil
}

func (p *Parameter) inheritParamIfNeeded(ctx context.Context, view *View, resource *Resource, structType reflect.Type) error {
	if p.Ref == "" {
		return nil
	}

	param, err := resource._parameters.Lookup(p.Ref)
	if err != nil {
		return err
	}

	if err = param.Init(ctx, view, resource, structType); err != nil {
		return err
	}

	p.inherit(param)
	return nil
}

func (p *Parameter) inherit(param *Parameter) {
	p.Name = FirstNotEmpty(p.Name, param.Name)
	p.Description = FirstNotEmpty(p.Description, param.Description)
	p.Style = FirstNotEmpty(p.Style, param.Style)
	p.PresenceName = FirstNotEmpty(p.PresenceName, param.PresenceName)
	if p.Const == nil {
		p.Const = param.Const
	}

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil {
		p.Schema = param.Schema.copy()
	}

	if p.Output == nil {
		p.Output = param.Output
	}

	if p._view == nil {
		p._view = param._view
	}

	if p.ErrorStatusCode == 0 {
		p.ErrorStatusCode = param.ErrorStatusCode
	}
}

//Validate checks if parameter is valid
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

//View returns View related with Parameter if Location.Kind is set to data_view
func (p *Parameter) View() *View {
	return p._view
}

//Validate checks if Location is valid
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

func (p *Parameter) initSchema(resource *Resource, structType reflect.Type) error {
	if p.Schema == nil {
		if p.In.Kind == KindLiteral {
			p.Schema = NewSchema(reflect.TypeOf(p.Const))
		} else {
			return fmt.Errorf("parameter %v schema can't be empty", p.Name)
		}
	}

	if p.Schema.Type() != nil {
		return nil
	}

	if structType != nil {
		return p.initSchemaFromType(structType)
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
		return fmt.Errorf("parameter %v either schema DataType or Name has to be specified", p.Name)
	}

	schemaType := FirstNotEmpty(p.Schema.Name, p.Schema.DataType)
	if p.MaxAllowedRecords != nil && *p.MaxAllowedRecords > 1 {
		p.Schema.Cardinality = Many
	}

	if schemaType != "" {
		lookup, err := types.GetOrParseType(resource.LookupType, schemaType)
		if err != nil {
			return err
		}

		p.Schema.SetType(lookup)
		return nil

	}

	return p.Schema.Init(nil, nil, 0, resource, nil)
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
	p.ensureSelectorParamValue(selector)

	paramPtr, presencePtr := asValuesPtr(selector)

	value, err := p.setValue(ctx, value, paramPtr, converted, selector)
	if err != nil {
		return nil, err
	}

	p.UpdatePresence(presencePtr)
	return value, nil
}

func (p *Parameter) setValue(ctx context.Context, value interface{}, paramPtr unsafe.Pointer, converted bool, options ...interface{}) (interface{}, error) {
	aCodec := p.Output
	if converted {
		aCodec = nil
	}

	var codecFn config.CodecFn
	if aCodec != nil {
		codecFn = aCodec._codecFn
	}

	if codecFn != nil {
		convertedValue, err := codecFn(ctx, value, options...)
		if err != nil {
			return nil, err
		}

		p._valueAccessor.SetValue(paramPtr, convertedValue)
		return convertedValue, nil
	}

	return p._valueAccessor.SetConvertedAndGet(paramPtr, value, p.DateFormat)
}

func (p *Parameter) Set(selector *Selector, value interface{}) error {
	_, err := p.convertAndSet(context.Background(), selector, value, true)
	return err
}

func (p *Parameter) SetAndGet(selector *Selector, value interface{}) (interface{}, error) {
	return p.convertAndSet(context.Background(), selector, value, true)
}

func asValuesPtr(selector *Selector) (paramPtr unsafe.Pointer, presencePtr unsafe.Pointer) {
	paramPtr = xunsafe.AsPointer(selector.Parameters.Values)
	presencePtr = xunsafe.AsPointer(selector.Parameters.Has)
	return paramPtr, presencePtr
}

func (p *Parameter) SetPresenceField(structType reflect.Type) error {
	fields, err := p.pathFields(p.PresenceName, structType)
	if err != nil {
		return err
	}

	p._presenceAccessor = types.NewAccessor(fields, structType)

	return nil
}

func (p *Parameter) initCodec(resource *Resource, view *View, paramType reflect.Type) error {
	if p.Output == nil {
		return nil
	}

	if err := p.Output.Init(resource, view, paramType); err != nil {
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

func (p *Parameter) ensureSelectorParamValue(selector *Selector) {
	selector.Parameters.Init(p._owner)
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

func (p *Parameter) initParamBasedParameter(ctx context.Context, view *View, resource *Resource) error {
	param, err := resource.ParamByName(p.In.Name)
	if err != nil {
		return err
	}

	if err = param.Init(ctx, view, resource, nil); err != nil {
		return err
	}

	p.Schema = param.Schema.copy()
	p._dependsOn = param
	return nil
}

func (p *Parameter) Parent() *Parameter {
	return p._dependsOn
}

//ParametersIndex represents Parameter map indexed by Parameter.Name
type ParametersIndex map[string]*Parameter

//ParametersSlice represents slice of parameters
type ParametersSlice []*Parameter

func (p ParametersSlice) Len() int {
	return len(p)
}

func (p ParametersSlice) Less(i, j int) bool {
	if p[j].ErrorStatusCode == 401 {
		return false
	}

	if p[j].ErrorStatusCode == 403 {
		return p[i].ErrorStatusCode == 401
	}

	return true
}

func (p ParametersSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

//Index indexes parameters by Parameter.Name
func (p ParametersSlice) Index() (ParametersIndex, error) {
	result := ParametersIndex(make(map[string]*Parameter))
	for parameterIndex := range p {
		if err := result.Register(p[parameterIndex]); err != nil {
			return nil, err
		}
	}

	return result, nil
}

//Filter filters ParametersSlice with given Kind and creates Template
func (p ParametersSlice) Filter(kind Kind) ParametersIndex {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p ParametersIndex) merge(with ParametersIndex) {
	for s := range with {
		p[s] = with[s]
	}
}

//Lookup returns Parameter with given name
func (p ParametersIndex) Lookup(paramName string) (*Parameter, error) {

	if param, ok := p[paramName]; ok {
		return param, nil
	}

	return nil, fmt.Errorf("not found parameter %v", paramName)
}

//Register registers parameter
func (p ParametersIndex) Register(parameter *Parameter) error {
	if _, ok := p[parameter.Name]; ok {
		fmt.Printf("[WARN] parameter with %v name already exists in given resource", parameter.Name)
	}

	p[parameter.Name] = parameter
	return nil
}

//NewQueryLocation creates a query location
func NewQueryLocation(name string) *Location {
	return &Location{Name: name, Kind: KindQuery}
}

//NewBodyLocation creates a body location
func NewBodyLocation(name string) *Location {
	return &Location{Name: name, Kind: KindRequestBody}
}

//NewDataViewLocation creates a dataview location
func NewDataViewLocation(name string) *Location {
	return &Location{Name: name, Kind: KindDataView}
}

//NewStructQLLocation creates a structql
func NewStructQLLocation(name string) *Location {
	return &Location{Name: name, Kind: KindStructQL}
}

//WithParameterType returns schema type parameter option
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

//NewParameter creates a parameter
func NewParameter(name string, in *Location, opts ...ParameterOption) *Parameter {
	ret := &Parameter{Name: name, In: in}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
