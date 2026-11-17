package presencegen

import (
	"fmt"
	"go/ast"
	"reflect"

	"github.com/viant/datly/typecatalog"
)

type scalarComparison struct {
	slice, pointer bool
}

type scalarShape struct {
	kind       reflect.Kind
	comparable bool
	element    *scalarShape
}

func classifyScalar(field sourceField, local map[string]*sourceType, resolver *typecatalog.Resolver) (scalarComparison, error) {
	shape, err := resolveScalarShape(field.typeAST, field.imports, local, resolver, map[string]bool{})
	if err != nil {
		return scalarComparison{}, err
	}
	result := scalarComparison{}
	if shape.kind == reflect.Pointer {
		result.pointer = true
		shape = shape.element
	}
	if shape == nil {
		return result, fmt.Errorf("unresolved pointer element")
	}
	if shape.kind == reflect.Slice {
		if shape.element == nil || !shape.element.comparable {
			return result, fmt.Errorf("slice element is not comparable")
		}
		result.slice = true
		return result, nil
	}
	if !shape.comparable {
		return result, fmt.Errorf("value is not comparable")
	}
	return result, nil
}

// Resolve source-local definitions before imported identities. A named slice
// retains its public Go spelling; only its comparison operation is selected
// from the underlying type. Imported types use the same catalog as generation.
func resolveScalarShape(expr ast.Expr, imports map[string]string, local map[string]*sourceType, resolver *typecatalog.Resolver, active map[string]bool) (*scalarShape, error) {
	switch value := expr.(type) {
	case *ast.ParenExpr:
		return resolveScalarShape(value.X, imports, local, resolver, active)
	case *ast.Ident:
		if definition := local[value.Name]; definition != nil {
			if active[value.Name] {
				return nil, fmt.Errorf("cyclic local type %s", value.Name)
			}
			active[value.Name] = true
			defer delete(active, value.Name)
			return resolveScalarShape(definition.underlying, definition.imports, local, resolver, active)
		}
		if primitiveType(expr) {
			return &scalarShape{comparable: true}, nil
		}
		return catalogScalarShape(value.Name, resolver)
	case *ast.SelectorExpr:
		qualifier, ok := value.X.(*ast.Ident)
		if !ok || imports[qualifier.Name] == "" {
			return nil, fmt.Errorf("unresolved imported type")
		}
		return catalogScalarShape(imports[qualifier.Name]+"."+value.Sel.Name, resolver)
	case *ast.StarExpr:
		// A pointer field within a recursive struct is comparable regardless of
		// its pointee. Do not recursively reconstruct that struct indefinitely.
		if name, ok := value.X.(*ast.Ident); ok && active[name.Name] {
			return &scalarShape{kind: reflect.Pointer, comparable: true}, nil
		}
		element, err := resolveScalarShape(value.X, imports, local, resolver, active)
		return &scalarShape{kind: reflect.Pointer, comparable: true, element: element}, err
	case *ast.ArrayType:
		element, err := resolveScalarShape(value.Elt, imports, local, resolver, active)
		if err != nil {
			return nil, err
		}
		if value.Len == nil {
			return &scalarShape{kind: reflect.Slice, element: element}, nil
		}
		return &scalarShape{kind: reflect.Array, comparable: element.comparable, element: element}, nil
	case *ast.StructType:
		result := &scalarShape{kind: reflect.Struct, comparable: true}
		for _, field := range value.Fields.List {
			shape, err := resolveScalarShape(field.Type, imports, local, resolver, active)
			if err != nil {
				return nil, err
			}
			result.comparable = result.comparable && shape.comparable
		}
		return result, nil
	case *ast.ChanType:
		return &scalarShape{kind: reflect.Chan, comparable: true}, nil
	default:
		return nil, fmt.Errorf("unsupported scalar type %T", expr)
	}
}

func catalogScalarShape(expression string, resolver *typecatalog.Resolver) (*scalarShape, error) {
	typ, err := resolver.Type(expression)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, fmt.Errorf("type %s has no resolved comparison authority", expression)
	}
	return reflectedScalarShape(typ), nil
}

func reflectedScalarShape(typ reflect.Type) *scalarShape {
	result := &scalarShape{kind: typ.Kind(), comparable: typ.Comparable()}
	if typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		result.element = reflectedScalarShape(typ.Elem())
	}
	return result
}
