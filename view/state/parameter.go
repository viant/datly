package state

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

type (
	Parameter struct {
		shared.Reference
		Object   Parameters `json:",omitempty" yaml:"Object"`
		Repeated Parameters `json:",omitempty" yaml:"Repeated"`

		//LocationInput, component input
		LocationInput *Type `json:",omitempty" yaml:"Input"`

		Predicates        []*extension.PredicateConfig `json:",omitempty" yaml:"Predicates"`
		Name              string                       `json:",omitempty" yaml:"Name"`
		SQL               string                       `json:",omitempty" yaml:"SQL"`
		In                *Location                    `json:",omitempty" yaml:"In" `
		Scope             string                       `json:",omitempty" yaml:"Scope" `
		Required          *bool                        `json:",omitempty"  yaml:"Required" `
		Description       string                       `json:",omitempty" yaml:"Documentation"`
		Style             string                       `json:",omitempty" yaml:"Style"`
		MaxAllowedRecords *int                         `json:",omitempty"`
		MinAllowedRecords *int                         `json:",omitempty"`
		ExpectedReturned  *int                         `json:",omitempty"`
		Schema            *Schema                      `json:",omitempty" yaml:"Schema"`
		Output            *Codec                       `json:",omitempty" yaml:"Output"`
		Handler           *Handler                     `json:",omitempty" yaml:"Handler"`
		Value             interface{}                  `json:"Value,omitempty" yaml:"Value"`
		//deprecated use format timelayout instead
		DateFormat      string `json:",omitempty" yaml:"DateFormat"`
		ErrorStatusCode int    `json:",omitempty" yaml:"ErrorStatusCode"`
		ErrorMessage    string `json:",omitempty" yaml:"ErrorMessage"`
		Tag             string `json:",omitempty" yaml:"Tag"`
		When            string `json:",omitempty" yaml:"When"`
		With            string `json:",omitempty" yaml:"With"`
		Cacheable       *bool  `json:",omitempty" yaml:"Cacheable"`

		isOutputType bool
		_timeLayout  string
		_selector    *structology.Selector
		_initialized bool
		_dependsOn   *Parameter
		_state       *structology.StateType
	}
	ParameterOption func(p *Parameter)
)

func (p *Parameter) IsUsedBy(text string) bool {
	parameter := p.Name
	text = strings.ReplaceAll(text, "Unsafe.", "")
	if index := strings.Index(text, "${"+parameter); index != -1 {
		match := text[index+2:]
		if index := strings.Index(match, "}"); index != -1 {
			match = match[:index]
		}
		if p.Name == match {
			return true
		}
	}
	for i := 0; i < len(text); i++ {
		index := strings.Index(text, "$"+parameter)
		if index == -1 {
			break
		}
		text = text[index+1:]
		if len(parameter) == len(text) {
			return true
		}
		terminator := text[len(parameter)]
		if terminator >= 65 && terminator <= 132 {
			continue
		}
		switch terminator {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			continue
		}
		return true
	}
	return false
}
func (p *Parameter) SetTypeNameTag() {
	schema := p.OutputSchema()
	if schema == nil {
		return
	}
	if _, ok := reflect.StructTag(p.Tag).Lookup(xreflect.TagTypeName); ok {
		return
	}
	p.Tag += " " + xreflect.TagTypeName + `:"` + schema.Name + `"`
}

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

	if err := p.inheritParamIfNeeded(ctx, resource); err != nil {
		return err
	}
	if p.In.Kind == KindLiteral {
		p.In.Kind = KindConst
	}

	if p.In.Kind == KindConst {
		if text, ok := p.Value.(string); ok {
			p.Value = resource.ExpandSubstitutes(text)
		}
	}
	if err := p.initObjectParams(ctx, resource); err != nil {
		return err
	}

	if err := p.initRepeatedParams(ctx, resource); err != nil {
		return err
	}

	if p.In == nil {
		return fmt.Errorf("parameter %v In can't be empty", p.Name)
	}

	p.In.Kind = Kind(strings.ToLower(string(p.In.Kind)))

	if p.In.Kind == KindConst && p.Value == nil {
		return fmt.Errorf("param %v value was not set", p.Name)
	}
	if p.In.IsView() {
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
	p._initialized = true
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
	setter.SetStringIfEmpty(&p.Tag, param.Tag)
	setter.SetStringIfEmpty(&p.When, param.When)
	setter.SetStringIfEmpty(&p.Scope, param.Scope)
	setter.SetStringIfEmpty(&p.With, param.With)
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
	if p.ErrorMessage == "" {
		p.ErrorMessage = param.ErrorMessage
	}

	if p.Predicates == nil {
		p.Predicates = param.Predicates
	}
	if p.Value == nil {
		p.Value = param.Value
	}

	if len(p.Object) == 0 {
		p.Object = param.Object
	}
	if len(p.Repeated) == 0 {
		p.Repeated = param.Repeated
	}
	if p.LocationInput == nil {
		p.LocationInput = param.LocationInput
	}
	if p.Handler == nil {
		p.Handler = param.Handler
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

		if p.Schema != nil && p.Schema.DataType != "" {
			if rType, err := resource.LookupType()(p.Schema.DataType, xreflect.WithPackage(p.Schema.Package)); err == nil {
				if p.Schema == nil {
					p.Schema = &Schema{}
				}
				p.Schema.SetType(rType)
			}
		}

		if p.Schema == nil || p.Schema.Type() == nil {
			if err := p.initObjectSchema(resource); err != nil {
				return err
			}
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
			p.Schema = NewSchema(reflect.TypeOf(p.Value))
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
		p.Schema = NewSchema(reflect.TypeOf(p.Value))
		return nil
	}

	if p.Schema == nil {
		return fmt.Errorf("parameter %v schema can't be empty", p.Name)
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

	rawItem := itemType
	if rawItem.Kind() == reflect.Ptr {
		rawItem = rawItem.Elem()
	}
	for _, item := range p.Repeated {
		elemType := item.OutputType()
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}
		if !rawItem.AssignableTo(elemType) {
			return fmt.Errorf("incompatible repeated type: %s, expected: %s, but had: %s -> %s", item.Name, itemType.String(), item.Name, item.OutputType().String())
		}
	}
	p.Schema = NewSchema(rType)
	return nil
}

func (p *Parameter) initObjectSchema(resource Resource) (err error) {
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
		var opts = []ReflectOption{WithTypeName(SanitizeTypeName(p.Name))}
		if !p.isOutputType {
			opts = append(opts, WithSetMarker())
		}
		if rType, err = p.Object.ReflectType(pkgPath, resource.LookupType(), opts...); err != nil {
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

	inputType := p.Schema.Type()
	if err := p.Output.Init(resource, inputType); err != nil {
		return err
	}
	if p.Output.Schema == nil {
		return nil
	}
	rType := p.Output.Schema.Type()

	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Struct {
		if rType.Name() == "" && p.Output.Schema.Name != "" {
			fieldTag := reflect.StructTag(p.Tag)
			if stateTag, _ := tags.ParseStateTags(fieldTag, nil); stateTag != nil {
				stateTag.TypeName = SanitizeTypeName(p.Output.Schema.Name)
				p.Tag = string(stateTag.UpdateTag(fieldTag))
			}
		}
	}
	return nil
}

func (p *Parameter) OutputType() reflect.Type {
	if p.Output != nil && p.Output.Schema != nil {
		if rType := p.Output.Schema.Type(); rType != nil {
			return rType
		}
	}
	return p.Schema.Type()
}

func (p *Parameter) initParamBasedParameter(ctx context.Context, resource Resource) error {
	if p.Schema.Type() != nil {
		return nil
	}
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

func (p *Parameter) initObjectParams(ctx context.Context, resource Resource) error {
	for _, parameter := range p.Object {
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

func (p *Parameter) CombineTags() reflect.StructTag {
	if p.Description == "" {
		return reflect.StructTag(p.Tag)
	}

	return reflect.StructTag(p.Tag + " description:" + strconv.Quote(p.Description))
}
