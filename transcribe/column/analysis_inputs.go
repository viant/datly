package column

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	veltyexpr "github.com/viant/velty/ast/expr"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// Declared inputs are parameterized by sql/template, even when their runtime
// value type is unknown here. Only plain member access is analyzed; no codec or
// template method is invoked to obtain metadata.
type analysisInputs struct {
	parameters map[string]*spec.Parameter
	types      *typecatalog.Resolver
}

func newAnalysisInputs(component *spec.Component, resolver *typecatalog.Resolver) *analysisInputs {
	result := &analysisInputs{parameters: map[string]*spec.Parameter{}, types: resolver}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param != nil && param.Name != "" && param.Source.Kind != "output" {
			result.parameters[param.Name] = param
		}
	}
	return result
}

func (a *analysisInputs) validate(selector *veltyexpr.Select) (string, error) {
	var names []string
	for current := selector; current != nil; {
		if !token.IsIdentifier(current.ID) {
			return "", fmt.Errorf("unsupported input value expression")
		}
		names = append(names, current.ID)
		if current.X == nil {
			break
		}
		next, ok := current.X.(*veltyexpr.Select)
		if !ok {
			return "", fmt.Errorf("unsupported input value expression %s: only member access can be analyzed", strings.Join(names, "."))
		}
		current = next
	}
	path := strings.Join(names, ".")
	if selector.ID == "Unsafe" || selector.ID == "View" || selector.ID == "criteria" || selector.ID == "SQLBindings" || selector.ID == "SQLInstanceConstants" {
		return "", fmt.Errorf("unsupported dynamic SQL structure: %s is not an input value", path)
	}
	param := a.parameters[selector.ID]
	if param == nil {
		return "", fmt.Errorf("undeclared input value %s", path)
	}
	typeExpr := param.OutputTypeExpr
	if typeExpr == "" && param.Codec != nil {
		typeExpr = param.Codec.OutputType
	} else if typeExpr == "" {
		typeExpr = param.TypeExpr
	}
	if err := a.validateMembers(typeExpr, nil, names[1:]); err != nil {
		return "", fmt.Errorf("input value %s: %w", path, err)
	}
	return path, nil
}

func (a *analysisInputs) validateMembers(expression string, linked reflect.Type, names []string) error {
	for index, name := range names {
		var descriptor *x.Type
		var err error
		if linked != nil {
			for linked.Kind() == reflect.Pointer {
				linked = linked.Elem()
			}
			if linked.Kind() == reflect.Interface {
				return nil // The runtime binder, not analysis, owns the dynamic value.
			}
			if linked.Kind() != reflect.Struct {
				return fmt.Errorf("member %s does not belong to struct type %s", name, linked)
			}
			descriptor = x.NewType(linked)
		} else {
			expression = strings.TrimSpace(expression)
			if expression == "" || expression == "any" || expression == "interface{}" {
				return nil
			}
			if object := types.Universe.Lookup(strings.TrimLeft(expression, "*")); object != nil {
				return fmt.Errorf("member %s does not belong to struct type %s", name, expression)
			}
			if strings.HasPrefix(expression, "[]") || strings.HasPrefix(expression, "map[") || strings.HasPrefix(expression, "*[]") {
				return fmt.Errorf("member %s requires a struct, not %s", name, expression)
			}
			if a.types != nil {
				descriptor, err = a.types.Descriptor(expression)
				if err != nil {
					return err
				}
			}
			if descriptor == nil {
				// The declared root still establishes binding as a value. Do not
				// manufacture a runtime type merely to validate SQL projection.
				return nil
			}
			if descriptor.Type != nil {
				return a.validateMembers("", descriptor.Type, names[index:])
			}
			if source := descriptor.SynteticType; source != nil && source.TypeSpec != nil {
				if _, ok := source.TypeSpec.Type.(*ast.InterfaceType); ok {
					return nil
				}
			}
		}
		var lookup xshape.Lookup
		if a.types != nil {
			lookup = a.types.Descriptor
		}
		fields, err := xshape.New(descriptor, lookup).Fields()
		if err != nil {
			return fmt.Errorf("resolve member %s: %w", name, err)
		}
		var found *xshape.Field
		for i := range fields {
			if fields[i].Name == name && fields[i].Exported {
				found = &fields[i]
				break
			}
		}
		if found == nil {
			return fmt.Errorf("exported member %s was not found", name)
		}
		linked = found.ReflectedType
		if linked == nil {
			expression, err = found.CanonicalType()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
