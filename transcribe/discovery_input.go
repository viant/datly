package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/xform/conv"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/toolbox"
)

type discoveryInputCompiler struct {
	component    *spec.Component
	declarations gen.Declarations
	viewBindings gen.ViewBindings
	resolver     *typecatalog.Resolver
	source       *Source
}

func (c *discoveryInputCompiler) compile() (*column.TemplateInput, error) {
	var instance *constant.Values
	if c.source != nil {
		instance = c.source.Const
	}
	staged := *c
	if instance != nil {
		staged.component = instance.Apply(c.component)
	}
	c = &staged
	constants, err := instance.For(c.component)
	if err != nil {
		return nil, err
	}
	inputType, err := c.inputType()
	if err != nil {
		return nil, err
	}
	inputType = indirectInputType(inputType)
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("transcribe column: input contract must resolve to a struct, got %v", inputType)
	}
	value := reflect.New(inputType).Elem()
	fieldIndex, err := tag.NewBindingIndex(inputType)
	if err != nil {
		return nil, fmt.Errorf("transcribe column: index input fields: %w", err)
	}
	projected := make([]bindly.ProjectionField, 0, inputType.NumField())
	variables := make([]sqltemplate.Variable, 0, inputType.NumField())
	for _, param := range spec.EffectiveParameters(c.component.Parameters) {
		if param == nil || param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		field, ok, err := fieldIndex.Resolve(param)
		if err != nil {
			return nil, fmt.Errorf("transcribe column: resolve input field for parameter %q: %w", param.Name, err)
		}
		if !ok {
			if param.IsTransportInput() {
				return nil, fmt.Errorf("transcribe column: input field for parameter %q was not found in %s", param.Name, inputType)
			}
			continue
		}
		variables = append(variables, sqltemplate.Variable{Name: param.Name, FieldIndex: field.Index, Type: field.Type})
		projected = append(projected, bindly.ProjectionField{
			Path: field.Name, Names: tag.BindingAliases(field, param),
		})
		if param.Value == nil {
			continue
		}
		target, err := value.FieldByIndexErr(field.Index)
		if err != nil {
			return nil, fmt.Errorf("transcribe column: resolve default field %s: %w", field.Name, err)
		}
		if !target.CanAddr() {
			return nil, fmt.Errorf("transcribe column: default field %s is not addressable", field.Name)
		}
		if strings.EqualFold(param.Source.Kind, "const") {
			converted, convertErr := (conv.ValueConverter{}).Convert(*param.Value, target.Type())
			if convertErr != nil {
				return nil, fmt.Errorf("transcribe column: constant %s cannot convert to %s", param.Name, target.Type())
			}
			target.Set(reflect.ValueOf(converted))
			continue
		}
		if err = toolbox.DefaultConverter.AssignConverted(target.Addr().Interface(), *param.Value); err != nil {
			return nil, fmt.Errorf("transcribe column: convert default for parameter %s: %w", param.Name, err)
		}
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, fmt.Errorf("transcribe column: create input projector: %w", err)
	}
	plan, err := injector.CompilePlan(inputType)
	if err != nil {
		return nil, fmt.Errorf("transcribe column: compile input projection plan: %w", err)
	}
	projection, err := plan.Projection(projected...)
	if err != nil {
		return nil, fmt.Errorf("transcribe column: compile input projection: %w", err)
	}
	resolver := discoveryParameterResolver(projection.Resolver(value.Addr().Interface()), inputDiscoveryTypes(inputType, c.component.Parameters, fieldIndex))
	return &column.TemplateInput{Const: constants, Value: value, Variables: variables, ParameterResolver: resolver}, nil
}

func inputDiscoveryTypes(inputType reflect.Type, params []*spec.Parameter, fieldIndex *tag.BindingIndex) map[string]reflect.Type {
	if inputType == nil || fieldIndex == nil {
		return nil
	}
	result := map[string]reflect.Type{}
	for _, param := range spec.EffectiveParameters(params) {
		if param == nil || param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		field, ok, err := fieldIndex.Resolve(param)
		if err != nil || !ok {
			continue
		}
		for _, alias := range tag.BindingAliases(field, param) {
			if key := strings.ToLower(strings.TrimSpace(alias)); key != "" {
				result[key] = field.Type
			}
		}
	}
	return result
}

func discoveryParameterResolver(delegate func(string) (any, bool, error), types map[string]reflect.Type) sqlx.ParameterResolver {
	return sqlx.ParameterResolver(func(name string) (any, bool, error) {
		value, ok, err := delegate(name)
		if err != nil || !ok {
			return value, ok, err
		}
		rType := types[strings.ToLower(strings.TrimSpace(name))]
		if replacement, replaced := discoveryValue(value, rType); replaced {
			return replacement, true, nil
		}
		return value, true, nil
	})
}

func discoveryValue(value any, rType reflect.Type) (any, bool) {
	if rType == nil {
		return nil, false
	}
	if value != nil {
		actual := reflect.ValueOf(value)
		switch actual.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if !actual.IsNil() {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	for rType.Kind() == reflect.Pointer {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Interface {
		return nil, false
	}
	if rType.Kind() == reflect.Slice {
		return reflect.MakeSlice(rType, 0, 0).Interface(), true
	}
	return reflect.Zero(rType).Interface(), true
}

func (c *discoveryInputCompiler) inputType() (reflect.Type, error) {
	if c.component == nil {
		return nil, fmt.Errorf("transcribe column: component is required")
	}
	if c.linkedInputAllowed() && c.component.Settings != nil && c.resolver != nil {
		expression := strings.TrimSpace(c.component.Settings.InputType)
		if expression != "" {
			linked, err := c.resolver.Type(expression)
			if err != nil {
				return nil, fmt.Errorf("transcribe column: resolve linked input %q: %w", expression, err)
			}
			if linked != nil {
				return linked, nil
			}
		}
	}
	targetPackage := ""
	if c.source != nil {
		targetPackage = strings.TrimSpace(c.source.Scope)
	}
	return gen.New(gen.Input{
		Component: c.component, Declarations: c.declarations,
		TypeResolver: c.resolver, TargetPackage: targetPackage, ViewBindings: c.viewBindings,
	}).RuntimeInputType()
}

func (c *discoveryInputCompiler) linkedInputAllowed() bool {
	if c.source == nil || c.source.PackageComponent == nil {
		return true
	}
	return (&contractLinker{packageComponent: c.source.PackageComponent, compiledComponent: c.component}).equalParams(false)
}

func indirectInputType(inputType reflect.Type) reflect.Type {
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	return inputType
}
