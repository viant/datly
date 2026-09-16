package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// EntityHookRequest carries canonical entity/parent identities, not runtime
// values. Parent is empty only at the root, whose typed parent is NoParent.
type EntityHookRequest struct{ Hook, Entity, Parent, Input, Output string }

// EntityHookCompiler validates authored hook contracts before target lowering.
// Instantiation and Bindly invocation lifetime belong to the generated program.
type EntityHookCompiler struct {
	Types *typecatalog.Resolver
	// CanonicalType applies generated alias authority when declarations use
	// package-local aliases for separately owned entities or contracts.
	CanonicalType func(string) (string, error)
}

func (c EntityHookCompiler) Compile(request EntityHookRequest) (spec.TypeRef, error) {
	if c.Types == nil {
		return spec.TypeRef{}, fmt.Errorf("entity hook %q requires canonical type authority", request.Hook)
	}
	resolved, err := c.Types.ResolveShape(request.Hook)
	if err != nil {
		return spec.TypeRef{}, err
	}
	if resolved == nil || resolved.Descriptor == nil {
		return spec.TypeRef{}, fmt.Errorf("entity hook type %q was not found", request.Hook)
	}
	reference, err := (xshape.Resolver{}).Reference(resolved.Identity)
	if err != nil {
		return spec.TypeRef{}, err
	}
	if len(reference.Wrappers) > 0 {
		return spec.TypeRef{}, fmt.Errorf("entity hook %q must name a concrete hook type without wrappers", request.Hook)
	}
	entity, err := (xshape.Resolver{}).Canonical(request.Entity)
	if err != nil || strings.TrimSpace(entity) == "" {
		return spec.TypeRef{}, fmt.Errorf("entity hook %q requires a canonical entity type: %v", request.Hook, err)
	}
	parent := request.Parent
	if parent == "" {
		parent = "github.com/viant/xdatly/handler.NoParent"
	}
	parent, err = (xshape.Resolver{}).Canonical(parent)
	if err != nil {
		return spec.TypeRef{}, err
	}
	output, err := (xshape.Resolver{}).Canonical(request.Output)
	if err != nil || strings.TrimSpace(output) == "" {
		return spec.TypeRef{}, fmt.Errorf("entity hook %q requires a canonical output type: %v", request.Hook, err)
	}
	shape := xshape.New(resolved.Descriptor, c.Types.Descriptor)
	methods, err := shape.Methods(true)
	if err != nil {
		return spec.TypeRef{}, fmt.Errorf("entity hook %q: %w", request.Hook, err)
	}
	if c.CanonicalType != nil {
		for i := range methods {
			for j, parameter := range methods[i].Parameters {
				parameter, err = c.CanonicalType(parameter)
				if err != nil {
					return spec.TypeRef{}, err
				}
				methods[i].Parameters[j], err = (xshape.Resolver{}).Canonical(parameter)
				if err != nil {
					return spec.TypeRef{}, err
				}
			}
		}
	}
	lifecycle := "github.com/viant/xdatly/handler.LifecycleContext[" + entity + "," + parent + "," + output + "]"
	expected := []string{"context.Context", "*" + entity, lifecycle}
	for index, name := range []string{"Init", "Validate", "AfterSequence", "AfterQueue"} {
		var found *xshape.Method
		for i := range methods {
			if methods[i].Name == name {
				found = &methods[i]
				break
			}
		}
		if found == nil {
			if index >= 2 {
				continue
			}
			return spec.TypeRef{}, fmt.Errorf("entity hook %s requires %s(context.Context, *%s, handler.LifecycleContext[%s,%s,%s]) error", resolved.Identity, name, entity, entity, parent, output)
		}
		if found.Variadic || !reflect.DeepEqual(found.Parameters, expected) || !reflect.DeepEqual(found.Results, []string{"error"}) {
			return spec.TypeRef{}, fmt.Errorf("entity hook %s.%s has incompatible signature: parameters=%v results=%v; expected parameters=%v and error result", resolved.Identity, name, found.Parameters, found.Results, expected)
		}
	}
	if request.Input != "" {
		input, err := (xshape.Resolver{}).Canonical(request.Input)
		if err != nil {
			return spec.TypeRef{}, err
		}
		for _, method := range methods {
			if method.Name != "Finalize" {
				continue
			}
			expected := []string{"context.Context", "*" + input, "*" + output, "github.com/viant/xdatly/handler.Outcome"}
			if method.Variadic || !reflect.DeepEqual(method.Parameters, expected) || !reflect.DeepEqual(method.Results, []string{"error"}) {
				return spec.TypeRef{}, fmt.Errorf("entity hook %s.Finalize has incompatible component completion signature", resolved.Identity)
			}
		}
	}
	return spec.TypeRef{Package: reference.Qualifier, Name: reference.Name}, nil
}
