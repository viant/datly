package transcribe

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/xform/conv"
	"github.com/viant/datly/constant"
	runtimecompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/toolbox"
	xshape "github.com/viant/x/shape"
)

type discoveryInputCompiler struct {
	component    *spec.Component
	declarations gen.Declarations
	viewBindings gen.ViewBindings
	resolver     *typecatalog.Resolver
	source       *Source
	factoryShape *gen.ExternalHandler
	views        gen.ViewReferences
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
			if param.Codec != nil && strings.TrimSpace(param.TypeExpr) != "" {
				resolver := c.resolver
				if resolver == nil {
					resolver, err = typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
					if err != nil {
						return nil, err
					}
				}
				sourceType, resolveErr := (xshape.Runtime{Lookup: resolver.Type}).Type(param.TypeExpr)
				if resolveErr != nil || sourceType == nil {
					return nil, fmt.Errorf("transcribe column: constant %s source type %q cannot resolve: %v", param.Name, param.TypeExpr, resolveErr)
				}
				if _, convertErr := (conv.ValueConverter{}).Convert(*param.Value, sourceType); convertErr != nil {
					return nil, fmt.Errorf("transcribe column: constant %s cannot convert to source %s", param.Name, sourceType)
				}
				// The literal belongs to the codec's source. Discovery must not
				// execute request codecs or assign it to their destination field.
				// Retain the typed destination's zero discovery value instead.
				continue
			}
			converted, convertErr := (conv.ValueConverter{}).Convert(*param.Value, target.Type())
			if convertErr != nil {
				return nil, fmt.Errorf("transcribe column: constant %s cannot convert to %s", param.Name, target.Type())
			}
			target.Set(reflect.ValueOf(converted))
			continue
		}
		collectionType := target.Type()
		for collectionType.Kind() == reflect.Pointer {
			collectionType = collectionType.Elem()
		}
		if collectionType.Kind() == reflect.Slice || collectionType.Kind() == reflect.Array {
			var converted any
			var convertErr error
			if strings.EqualFold(param.Source.Kind, "query") && param.Codec == nil {
				// Compile just this field through the existing type owner so discovery
				// uses query-list conversion without unrelated binding tags or codecs.
				fieldType, fieldErr := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: field.Name, Type: field.Type, Tag: field.Tag}})
				if fieldErr != nil {
					return nil, fmt.Errorf("transcribe column: compile default field %s: %w", field.Name, fieldErr)
				}
				bindings, bindingErr := runtimecompiler.BuildBindingSpecs(
					&spec.Component{Parameters: []*spec.Parameter{param}},
					fieldType, nil)
				if bindingErr != nil {
					return nil, fmt.Errorf("transcribe column: compile default binding for parameter %s: %w", param.Name, bindingErr)
				}
				if len(bindings) == 1 && bindings[0].Transformer != nil {
					converted, convertErr = bindings[0].Transformer.Transform(context.Background(), nil, *param.Value)
				} else {
					converted, convertErr = (conv.ValueConverter{}).Convert(*param.Value, target.Type())
				}
			} else {
				converted, convertErr = (conv.ValueConverter{}).Convert(*param.Value, target.Type())
			}
			if convertErr != nil {
				return nil, fmt.Errorf("transcribe column: convert default for parameter %s: %w", param.Name, convertErr)
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
	// A Go-only component retains its exact linked contract, just as package
	// compilation retains that contract after discovery. Expanded defaults are
	// not an authored overlay requiring a synthetic replacement input.
	if c.source != nil && c.source.PackageComponent != nil && strings.TrimSpace(c.source.Text) == "" && c.source.LinkedInputType != nil {
		return c.source.LinkedInputType, nil
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
	// The source namespace may be a DQL directory. Generated declarations
	// belong to the authored Go package, as they do in final generation.
	if c.component.TypeContext != nil && strings.TrimSpace(c.component.TypeContext.PackagePath) != "" {
		targetPackage = strings.TrimSpace(c.component.TypeContext.PackagePath)
	}
	component := c.component
	if c.factoryShape != nil {
		targetPackage = c.factoryShape.Package
		component = c.component.Clone()
		component.RootView = nil
	}
	input := gen.Input{
		ExternalHandler: c.factoryShape,
		Component:       component, Declarations: c.declarations,
		TypeResolver: c.resolver, TargetPackage: targetPackage, ViewBindings: c.viewBindings,
		Views: c.views,
	}
	if c.source != nil {
		input.Resources = c.source.Resources
		// Reuse the package compilation authority during provisional discovery,
		// before packageAuthority.compile attaches its final linked references.
		if c.source.PackageComponent != nil && c.factoryShape == nil && c.resolver != nil {
			var err error
			input.Views, err = packageInputViewReferences(component, c.source.PackageComponent, c.resolver, c.source.selfGeneratedInputViews)
			if err != nil {
				return nil, err
			}
		}
	}
	return gen.New(input).ColumnDiscoveryInputType()
}

func (c *discoveryInputCompiler) linkedInputAllowed() bool {
	if c.factoryShape != nil && c.factoryShape.GeneratedContracts {
		return false
	}
	if c.source == nil || c.source.PackageComponent == nil {
		return true
	}
	return (&link.Contracts{PackageComponent: c.source.PackageComponent, CompiledComponent: c.component}).EqualParams(false)
}

func indirectInputType(inputType reflect.Type) reflect.Type {
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	return inputType
}
