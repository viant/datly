package state

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
)

type (
	Parameter struct {
		shared.Reference
		Group      Parameters                   `json:",omitempty"`
		Repeated   Parameters                   `json:",omitempty" yaml:"Repeated"`
		Predicates []*extension.PredicateConfig `json:",omitempty" yaml:"Predicates"`
		Name       string                       `json:",omitempty" yaml:"Name"`

		In                *Location   `json:",omitempty" yaml:"In" `
		Required          *bool       `json:",omitempty"`
		Description       string      `json:",omitempty"`
		DataType          string      `json:",omitempty" `
		Style             string      `json:",omitempty"`
		MaxAllowedRecords *int        `json:",omitempty"`
		MinAllowedRecords *int        `json:",omitempty"`
		ExpectedReturned  *int        `json:",omitempty"`
		Schema            *Schema     `json:",omitempty" yaml:"Schema"`
		Output            *Codec      `json:",omitempty" yaml:"Output"`
		Const             interface{} `json:",omitempty" yaml:"Const"`
		Value             interface{} `json:"Value,omitempty" yaml:"Value"`
		DateFormat        string      `json:",omitempty" yaml:"DateFormat"`
		ErrorStatusCode   int         `json:",omitempty" yaml:"ErrorStatusCode"`
		Tag               string      `json:",omitempty" yaml:"Tag"`
		Lazy              bool        `json:",omitempty" yaml:"Lazy"`
		When              string      `json:",omitempty" yaml:"When"`
		Cacheable         *bool       `json:",omitempty" yaml:"Cacheable"`

		isOutputType bool

		_selector    *structology.Selector
		_initialized bool
		_dependsOn   *Parameter
		_state       *structology.StateType
	}
	ParameterOption func(p *Parameter)
)

func (p *Parameter) IsCacheable() bool {
	if p.Cacheable == nil {
		return p.In.Kind != KindState
	}
	return *p.Cacheable
}
func (p Parameters) FlagOutput() {
	for _, param := range p {
		param.isOutputType = true
	}
}

func (p *Parameter) Clone() *Parameter {
	ret := *p
	return &ret
}

func (p *Parameter) OutputSchema() *Schema {
	if p.Output != nil && p.Output.Schema != nil {
		return p.Output.Schema
	}
	return p.Schema
}

// Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, resource Resource) error {
	if p._initialized == true {
		return nil
	}
	p._initialized = true

	if err := p.inheritParamIfNeeded(ctx, resource); err != nil {
		return err
	}
	if p.In.Kind == KindLiteral {
		p.In.Kind = KindConst
	}

	if p.In.Kind == KindConst {
		if text, ok := p.Const.(string); ok {
			p.Const = resource.ExpandSubstitutes(text)
		}
	}

	if err := p.initGroupParams(ctx, resource); err != nil {
		return err
	}

	if err := p.initRepeatedParams(ctx, resource); err != nil {
		return err
	}

	if p.In == nil {
		return fmt.Errorf("parameter %v In can't be empty", p.Name)
	}

	p.In.Kind = Kind(strings.ToLower(string(p.In.Kind)))

	if p.In.Kind == KindConst && p.Const == nil {
		return fmt.Errorf("param %v value was not set", p.Name)
	}

	if p.In.Kind == KindDataView {
		if err := p.initDataViewParameter(ctx, resource); err != nil {
			return err
		}
	}

	switch p.In.Kind {
	case KindParam, KindState:
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

func (p *Parameter) initDataViewParameter(ctx context.Context, resource Resource) error {
	if p.Schema != nil && p.Schema.Type() != nil {
		return nil
	}
	schema, err := resource.ViewSchema(ctx, p.In.Name)
	if err != nil {
		return fmt.Errorf("failed to apply view parameter %v, %w", p.Name, err)
	}

	cardinality := Cardinality("")
	if p.Schema != nil {
		cardinality = p.Schema.Cardinality
	}
	p.Schema = schema.Clone()
	parameterType := schema.Type()
	if cardinality != "" {
		p.Schema.Cardinality = cardinality
		if cardinality == One && parameterType.Kind() == reflect.Slice {
			parameterType = parameterType.Elem()
		}

	}
	p.Schema.SetType(parameterType)
	return nil
}

func (p *Parameter) inheritParamIfNeeded(ctx context.Context, resource Resource) error {
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
	setter.SetStringIfEmpty(&p.Name, param.Name)
	setter.SetStringIfEmpty(&p.Description, param.Description)
	setter.SetStringIfEmpty(&p.Style, param.Style)
	setter.SetStringIfEmpty(&p.DataType, param.DataType)
	setter.SetStringIfEmpty(&p.Tag, param.Tag)
	setter.SetStringIfEmpty(&p.When, param.When)
	setter.SetBoolIfFalse(&p.Lazy, param.Lazy)

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
		p.Schema = param.Schema.Clone()
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
	if p.Value == nil {
		p.Value = param.Value
	}

	if len(p.Group) == 0 {
		p.Group = param.Group
	}
	if len(p.Repeated) == 0 {
		p.Repeated = param.Repeated
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

func (p *Parameter) IsRequired() bool {
	return p.Required != nil && *p.Required == true
}

func (p *Parameter) initSchema(resource Resource) error {
	if p.In.Kind == KindObject {
		if err := p.initGroupSchema(resource); err != nil {
			return err
		}
		p._state = structology.NewStateType(p.Schema.Type())
		p._state.NewState()
		return nil
	}

	if p.In.Kind == KindRepeated {
		err := p.initRepeatedSchema(resource)
		if err != nil {
			return err
		}
	}

	if p.In.Kind == KindRequest {
		p.Schema = NewSchema(reflect.TypeOf(&http.Request{}))
		return nil
	}

	if p.Schema == nil {
		if p.In.Kind == KindConst {
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

	if p.In.Kind == KindConst {
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

	if p.MaxAllowedRecords != nil && *p.MaxAllowedRecords > 1 {
		p.Schema.Cardinality = Many
	}

	if typeName := p.Schema.TypeName(); typeName != "" {
		lookup, err := types.LookupType(resource.LookupType(), typeName)
		if err != nil {
			return err
		}

		p.Schema.SetType(lookup)
		return nil

	}
	return p.Schema.Init(resource)
}

func (p *Parameter) initRepeatedSchema(resource Resource) (err error) {
	for _, item := range p.Repeated {
		if err := item.Schema.Init(resource); err != nil {
			return err
		}
	}

	var rType reflect.Type
	if typeName := p.Schema.TypeName(); typeName != "" {
		rType, err = resource.LookupType()(typeName)
		if err != nil {
			return err
		}
		if rType.Kind() != reflect.Slice {
			rType = reflect.SliceOf(rType)
		}
	}

	itemType := p.Repeated[0].OutputSchema().Type()
	if rType != nil {
		itemType = rType.Elem()
	} else {
		rType = reflect.SliceOf(itemType)
	}
	for _, item := range p.Repeated {
		if !itemType.AssignableTo(item.OutputType()) {
			return fmt.Errorf("incompatible repeated type: %s, expected: %s, but had: %s -> %s", item.Name, itemType.String(), item.Name, item.OutputType().String())
		}
	}
	p.Schema = NewSchema(rType)
	return nil
}

func (p *Parameter) initGroupSchema(resource Resource) (err error) {
	var rType reflect.Type
	if p.Schema == nil {
		p.Schema = &Schema{}
	}
	if typeName := p.Schema.TypeName(); typeName != "" {
		rType, err = resource.LookupType()(typeName)
		if err != nil {
			return err
		}
	}
	if rType == nil {
		if rType, err = p.Group.ReflectType(pkgPath, resource.LookupType(), !p.isOutputType); err != nil {
			return err
		}
	}
	p.Schema.SetType(rType)
	return nil
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

func (p *Parameter) GetValue(state *structology.State) (interface{}, error) {
	return p._selector.Value(state.Pointer()), nil
}

func (p *Parameter) Set(state *structology.State, value interface{}) error {
	return p._selector.SetValue(state.Pointer(), value)
}

func (p *Parameter) initCodec(resource Resource) error {
	if p.Output == nil {
		return nil
	}

	if err := p.Output.Init(resource, p.Schema.Type()); err != nil {
		return err
	}

	return nil
}

func (p *Parameter) OutputType() reflect.Type {
	if p.Output != nil && p.Output.Schema != nil {
		return p.Output.Schema.Type()
	}
	return p.Schema.Type()
}

func (p *Parameter) initParamBasedParameter(ctx context.Context, resource Resource) error {
	parameterName := p.In.Name
	var parameterSelectr string
	if index := strings.Index(parameterName, "."); index != -1 {
		parameterName = p.In.Name[:index]
		parameterSelectr = p.In.Name[index+1:]
	}

	param, err := resource.LookupParameter(parameterName)
	if err != nil {
		return err
	}

	if err = param.Init(ctx, resource); err != nil {
		return err
	}

	baseSchema := param.Schema.Clone()

	if parameterSelectr != "" {
		stateType := structology.NewStateType(param.OutputType())
		selector := stateType.Lookup(parameterSelectr)
		if selector == nil {
			return fmt.Errorf("invalid parameter %v path %v", p.Name, parameterSelectr)
		}
		baseSchema = NewSchema(selector.Type())
	}

	p.Schema = baseSchema
	p._dependsOn = param
	return nil
}

func (p *Parameter) Parent() *Parameter {
	return p._dependsOn
}

func (p *Parameter) SetSelector(selector *structology.Selector) {
	p._selector = selector
}

func (p *Parameter) Selector() *structology.Selector {
	return p._selector
}

func (p *Parameter) initGroupParams(ctx context.Context, resource Resource) error {
	for _, parameter := range p.Group {
		if err := parameter.Init(ctx, resource); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parameter) initRepeatedParams(ctx context.Context, resource Resource) error {
	for _, parameter := range p.Repeated {
		if err := parameter.Init(ctx, resource); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parameter) NewState(value interface{}) *structology.State {
	return p._state.WithValue(value)
}
