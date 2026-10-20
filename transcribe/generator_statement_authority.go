package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// Resolve only authored paths rooted in an already admitted contract field.
// Native shape resolution owns visibility, promotion and declaring authority.
func extendStatementAuthority(source string, authority map[string]plan.StatementSelector, contracts *gen.Plan, types *typecatalog.Resolver) error {
	imports := map[string]string{}
	for _, item := range contracts.Imports {
		imports[item.Alias] = item.Package
	}
	canonical := xshape.Resolver{Package: contracts.Package, Imports: imports}
	for _, path := range compiler.StatementSelectorPaths(source) {
		if len(path) < 3 {
			continue
		}
		key := strings.Join(path, ".")
		if _, found := authority[key]; found {
			continue
		}
		top, found := authority[strings.Join(path[:2], ".")]
		if !found {
			continue
		}
		if types == nil {
			return fmt.Errorf("statement selector %s requires canonical type metadata", key)
		}
		identity, err := canonical.Canonical(top.Type.Name)
		if err != nil {
			return fmt.Errorf("statement selector %s: %w", key, err)
		}
		descriptor, err := types.Descriptor(identity)
		if err != nil {
			return fmt.Errorf("statement selector %s: %w", key, err)
		}
		shape := xshape.New(descriptor, types.Descriptor)
		parents := []plan.FieldPath{}
		for i := 2; i < len(path); i++ {
			prefix := append(plan.FieldPath(nil), path[:i]...)
			pointer, err := statementStructPrefix(identity, shape)
			if err != nil {
				return fmt.Errorf("statement selector %s: %w", key, err)
			}
			if pointer {
				parents = append(parents, prefix)
			}
			// ResolveField retains declaring imports, including promoted fields. Resolve
			// each prefix as well so collections cannot acquire struct traversal by
			// shape's intentionally broader structural field discovery.
			fields, err := shape.Fields()
			if err != nil {
				return fmt.Errorf("statement selector %s: %w", key, err)
			}
			var selected *xshape.Field
			for n := range fields {
				if fields[n].Name == path[i] && fields[n].Exported {
					selected = &fields[n]
					break
				}
			}
			if selected == nil {
				return fmt.Errorf("statement selector %s: exported field %s was not found", key, path[i])
			}
			promoted, err := statementPromotedParents(shape, *selected, prefix, types)
			if err != nil {
				return fmt.Errorf("statement selector %s: %w", key, err)
			}
			parents = append(parents, promoted...)
			resolved, err := shape.ResolveField(path[i])
			if err != nil {
				return fmt.Errorf("statement selector %s: %w", key, err)
			}
			identity, descriptor = resolved.Identity, resolved.Descriptor
			shape = xshape.New(descriptor, types.Descriptor)
		}
		if !compiler.StatementScalarType(identity) {
			return fmt.Errorf("statement selector %s requires a scalar leaf, got %s", key, identity)
		}
		authority[key] = plan.StatementSelector{Path: append(plan.FieldPath(nil), path...), Type: spec.TypeRef{Name: identity}, Addressable: true, PointerParents: parents}
	}
	return nil
}

func statementStructPrefix(identity string, shape *xshape.Type) (bool, error) {
	ref, err := (xshape.Resolver{}).Reference(identity)
	if err != nil {
		return false, fmt.Errorf("non-struct traversal through %s", identity)
	}
	if len(ref.Wrappers) > 1 || (len(ref.Wrappers) == 1 && ref.Wrappers[0].Kind != xshape.WrapperPointer) {
		return false, fmt.Errorf("unsupported traversal through %s", identity)
	}
	structural, err := shape.IsStruct()
	if err != nil {
		return false, err
	}
	if !structural {
		return false, fmt.Errorf("non-struct traversal through %s", identity)
	}
	return len(ref.Wrappers) == 1, nil
}

// Visible-field indexes expose implicit embedded parents. Guard their pointer
// prefixes too, without changing Go's field-selection precedence.
func statementPromotedParents(shape *xshape.Type, field xshape.Field, prefix plan.FieldPath, types *typecatalog.Resolver) ([]plan.FieldPath, error) {
	var parents []plan.FieldPath
	for _, index := range field.Index[:len(field.Index)-1] {
		fields, err := shape.Fields()
		if err != nil {
			return nil, err
		}
		var embedded *xshape.Field
		for n := range fields {
			if len(fields[n].Index) == 1 && fields[n].Index[0] == index {
				embedded = &fields[n]
				break
			}
		}
		if embedded == nil || !embedded.Exported {
			return nil, fmt.Errorf("embedded parent is not accessible")
		}
		resolved, err := shape.ResolveField(embedded.Name)
		if err != nil {
			return nil, err
		}
		prefix = append(append(plan.FieldPath(nil), prefix...), embedded.Name)
		shape = xshape.New(resolved.Descriptor, types.Descriptor)
		pointer, err := statementStructPrefix(resolved.Identity, shape)
		if err != nil {
			return nil, err
		}
		if pointer {
			parents = append(parents, prefix)
		}
	}
	return parents, nil
}
