package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

const (
	VeltyCriteriaCodec = "VeltyCriteria"
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
		Schema            *Schema   `json:",omitempty"`
		Codec             *Codec    `json:",omitempty"`

		DateFormat      string `json:",omitempty"`
		ErrorStatusCode int    `json:",omitempty"`

		valueAccessor    *Accessor
		presenceAccessor *Accessor
		initialized      bool
		view             *View
		_owner           *View
	}

	//Location tells how to get parameter value.
	Location struct {
		Kind Kind   `json:",omitempty"`
		Name string `json:",omitempty"`
	}

	CodecFn func(context context.Context, rawValue interface{}, options ...interface{}) (interface{}, error)
	Codec   struct {
		shared.Reference
		Name      string  `json:",omitempty"`
		Source    string  `json:",omitempty"`
		SourceURL string  `json:",omitempty"`
		Schema    *Schema `json:",omitempty"`
		_codecFn  CodecFn //shall rename to codec ?
	}
)

func (v *Codec) Init(resource *Resource, view *View, paramType reflect.Type) error {
	if v.Ref != "" {
		visitor, ok := resource.VisitorByName(v.Ref)
		if !ok {
			return fmt.Errorf("not found visitor with name %v", v.Ref)
		}

		asCodec, ok := visitor.(codec.Codec)
		if !ok {
			return fmt.Errorf("expected visitor to be type of %T but was %T", asCodec, visitor)
		}

		v.inherit(asCodec)
	}

	v.ensureSchema(paramType)
	if v.SourceURL != "" && v.Source == "" {
		data, err := resource.LoadText(context.Background(), v.SourceURL)
		if err != nil {
			return err
		}
		v.Source = data
	}

	if err := v.Schema.Init(nil, nil, format.CaseUpperCamel, resource._types, nil); err != nil {
		return err
	}

	fn, err := v.extractCodecFn(resource, v.Schema.Type(), view)
	if err != nil {
		return err
	}

	v._codecFn = fn
	return nil
}

func (v *Codec) ensureSchema(paramType reflect.Type) {
	if v.Schema == nil {
		v.Schema = &Schema{}
		v.Schema.setType(paramType)
	}
}

func (v *Codec) extractCodecFn(resource *Resource, paramType reflect.Type, view *View) (CodecFn, error) {
	switch v.Name {
	case VeltyCriteriaCodec:
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
	case codec.LifecycleVisitor:
		return actual.Valuer().Value, nil
	case codec.Codec:
		return actual.Valuer().Value, nil
	default:
		return nil, fmt.Errorf("expected %T to implement Codec", actual)
	}
}

func (v *Codec) Transform(ctx context.Context, raw string, options ...interface{}) (interface{}, error) {
	return v._codecFn(ctx, raw, options...)
}

func (v *Codec) inherit(asCodec codec.Codec) {
	v.Name = asCodec.Name()
	v.Schema = NewSchema(asCodec.ResultType())
	v.Schema.DataType = asCodec.ResultType().String()
	v._codecFn = asCodec.Valuer().Value
}

//Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, view *View, resource *Resource, structType reflect.Type) error {
	if p.initialized == true {
		return nil
	}
	p.initialized = true
	p._owner = view

	if p.Ref != "" {
		param, err := resource._parameters.Lookup(p.Ref)
		if err != nil {
			return err
		}

		if err = param.Init(ctx, view, resource, structType); err != nil {
			return err
		}

		p.inherit(param)
	}

	if p.PresenceName == "" {
		p.PresenceName = p.Name
	}

	if p.In.Kind == DataViewKind {
		aView, err := resource.View(p.In.Name)
		if err != nil {
			return fmt.Errorf("failed to lookup parameter %v view %w", p.Name, err)
		}

		if err = aView.Init(ctx, resource); err != nil {
			return err
		}

		p.view = aView

		if p.Schema == nil {
			p.Schema = aView.Schema
		}
	}

	if err := p.initSchema(resource._types, structType); err != nil {
		return err
	}

	if err := p.initCodec(resource, view, p.Schema.Type()); err != nil {
		return err
	}

	return p.Validate()
}

func (p *Parameter) inherit(param *Parameter) {
	p.Name = NotEmptyOf(p.Name, param.Name)
	p.Description = NotEmptyOf(p.Description, param.Description)
	p.Style = NotEmptyOf(p.Style, param.Style)
	p.PresenceName = NotEmptyOf(p.PresenceName, param.PresenceName)

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil {
		p.Schema = param.Schema.copy()
	}

	if p.Codec == nil {
		p.Codec = param.Codec
	}

	if p.view == nil {
		p.view = param.view
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
	return p.view
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

func (p *Parameter) initSchema(types Types, structType reflect.Type) error {
	if structType != nil {
		return p.initSchemaFromType(structType)
	}

	if p.Schema == nil {
		if p.DataType != "" {
			p.Schema = &Schema{DataType: p.DataType}
		} else {
			return fmt.Errorf("parameter %v schema can't be empty", p.Name)
		}
	}

	if p.Schema.DataType == "" && p.Schema.Name == "" {
		return fmt.Errorf("parameter %v either schema DataType or DbName has to be specified", p.Name)
	}

	schemaType := NotEmptyOf(p.Schema.Name, p.Schema.DataType)
	if p.MaxAllowedRecords != nil && *p.MaxAllowedRecords > 1 {
		p.Schema.Cardinality = Many
	}

	if schemaType != "" {
		lookup, err := GetOrParseType(types, schemaType)
		if err != nil {
			return err
		}

		p.Schema.setType(lookup)
		return nil

	}

	return p.Schema.Init(nil, nil, 0, types, nil)
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

	p.Schema.setType(field.Type)
	return nil
}

func (p *Parameter) UpdatePresence(presencePtr unsafe.Pointer) {
	p.presenceAccessor.setBool(presencePtr, true)
}

func (p *Parameter) SetAccessor(accessor *Accessor) {
	p.valueAccessor = accessor
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
	return p.valueAccessor.Value(values)
}

func (p *Parameter) ConvertAndSet(ctx context.Context, selector *Selector, value string) error {
	p.ensureSelectorParamValue(selector)

	paramPtr, presencePtr := asValuesPtr(selector)

	if err := p.valueAccessor.setValue(ctx, paramPtr, value, p.Codec, selector, p.DateFormat); err != nil {
		return err
	}

	p.UpdatePresence(presencePtr)
	return nil
}

func elem(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func (p *Parameter) Set(selector *Selector, value interface{}) error {
	p.ensureSelectorParamValue(selector)

	ptr, presencePtr := asValuesPtr(selector)
	p.valueAccessor.set(ptr, value)
	p.UpdatePresence(presencePtr)

	return nil
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

	p.presenceAccessor = &Accessor{
		xFields: fields,
	}

	return nil
}

func (p *Parameter) initCodec(resource *Resource, view *View, paramType reflect.Type) error {
	if p.Codec == nil {
		return nil
	}

	if err := p.Codec.Init(resource, view, paramType); err != nil {
		return err
	}

	return nil
}

func (p *Parameter) ActualParamType() reflect.Type {
	if p.Codec != nil && p.Codec.Schema != nil {
		return p.Codec.Schema.Type()
	}

	return p.Schema.Type()
}

func (p *Parameter) ensureSelectorParamValue(selector *Selector) {
	selector.Parameters.Init(p._owner)
}

//ParametersIndex represents Parameter map indexed by Parameter.Name
type ParametersIndex map[string]*Parameter

//ParametersSlice represents slice of parameters
type ParametersSlice []*Parameter

//Index indexes parameters by Parameter.Name
func (p ParametersSlice) Index() ParametersIndex {
	result := ParametersIndex(make(map[string]*Parameter))
	for parameterIndex := range p {
		result.Register(p[parameterIndex])
	}

	return result
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
func (p ParametersIndex) Register(parameter *Parameter) {
	keys := shared.KeysOf(parameter.Name, false)
	for _, key := range keys {
		p[key] = parameter
	}
}

//NewQueryLocation creates a query location
func NewQueryLocation(name string) *Location {
	return &Location{Name: name, Kind: QueryKind}
}

func GetOrParseType(types Types, dataType string) (reflect.Type, error) {
	lookup, lookupErr := types.Lookup(dataType)
	if lookupErr == nil {
		return lookup, nil
	}

	parseType, parseErr := ParseType(dataType)
	if parseErr == nil {
		return parseType, nil
	}

	return nil, fmt.Errorf("couldn't determine struct type: %v, due to the: %w, %v", dataType, lookupErr, parseErr)
}
