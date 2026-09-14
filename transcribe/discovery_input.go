package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
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
		variables = append(variables, sqltemplate.Variable{Name: param.Name, FieldIndex: field.Index})
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
	resolver := sqlx.ParameterResolver(projection.Resolver(value.Addr().Interface()))
	return &column.TemplateInput{Value: value, Variables: variables, ParameterResolver: resolver}, nil
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
