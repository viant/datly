package transcribe

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// generatedRowAuthority records declarations, never synthetic catalog types.
// Only a uniquely declared local row may defer descriptor lookup until native
// column discovery and the ordinary generation planner materialize its shape.
type generatedRowAuthority struct {
	root         *spec.View
	plan         *gen.Plan
	owners       map[string][]*spec.View
	resolver     *typecatalog.Resolver
	views        []*spec.View
	dependencies map[*spec.View][]generatedColumnDependencies
}

// Dependencies retain column order and context across both validation passes.
type generatedColumnDependencies struct {
	name string
	keys []string
	err  error
}

func collectGeneratedRowAuthority(component *spec.Component, prepared *dql.PreparedSource, declarations *declarationProducts, resolver *typecatalog.Resolver, path string, sourceMap *SourceMap) (*generatedRowAuthority, error) {
	a := &generatedRowAuthority{owners: map[string][]*spec.View{}, resolver: resolver}
	if component.TypeContext == nil || component.TypeContext.PackagePath == "" {
		return a, nil
	}
	a.plan = &gen.Plan{Package: component.TypeContext.PackagePath, Imports: component.TypeContext.Imports}
	// Parse and lower the canonical graph once, including resolver-dependent hook
	// keys before source decomposition. Named CAST lookup is deferred until this
	// graph supplies SQL ownership; validateDependencies below checks every named
	// dependency before the graph can be returned to any downstream consumer.
	root, err := (&readPlanCompiler{path: path, sourceMap: sourceMap, types: resolver,
		generatedType: func(string) (bool, error) { return true, nil },
	}).compile(component, prepared)
	if err != nil {
		return nil, err
	}
	a.root = root
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		a.views = append(a.views, view)
		if view.TypeName != "" {
			key, err := a.plan.CanonicalType(a.plan.Package, view.TypeName)
			if err != nil {
				return err
			}
			name := strings.TrimPrefix(key, a.plan.Package+".")
			if name != key && token.IsIdentifier(name) && ast.IsExported(name) {
				a.owners[key] = append(a.owners[key], view)
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	if declarations != nil {
		for _, view := range declarations.views {
			if err := visit(view); err != nil {
				return nil, err
			}
		}
		for _, relation := range declarations.outputRelations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := a.validateCycles(); err != nil {
		return nil, err
	}
	if err := a.validateDependencies(); err != nil {
		return nil, err
	}
	return a, nil
}

// walkDependencies is the one native expression traversal used by both strict
// dependency validation and cycle checking, including structural field types.
func (a *generatedRowAuthority) walkDependencies(expression string, visit func(string) error) error {
	canonical, err := a.plan.CanonicalType(a.plan.Package, expression)
	if err != nil {
		return err
	}
	_, err = (xshape.Resolver{Rewriter: func(key string) (string, error) {
		if _, err := (xshape.Runtime{}).Type(key); err == nil {
			return key, nil
		}
		if err := visit(key); err != nil {
			return "", err
		}
		return key, nil
	}}).Rewrite(canonical)
	return err
}

func (a *generatedRowAuthority) walkColumns(view *spec.View, visit func(string) error) error {
	if a.dependencies == nil {
		a.dependencies = map[*spec.View][]generatedColumnDependencies{}
	}
	columns, ok := a.dependencies[view]
	if !ok {
		for _, column := range view.Columns {
			if column == nil || !column.ExplicitType || column.Type.Name == "" {
				continue
			}
			name := column.Type.Name
			if column.Type.Package != "" {
				name = column.Type.Package + "." + name
			}
			dependency := generatedColumnDependencies{name: column.Name}
			dependency.err = a.walkDependencies(name, func(key string) error {
				dependency.keys = append(dependency.keys, key)
				return nil
			})
			columns = append(columns, dependency)
		}
		a.dependencies[view] = columns
	}
	for _, column := range columns {
		for _, key := range column.keys {
			if err := visit(key); err != nil {
				return fmt.Errorf("generated row reference %s.%s: %w", view.Name, column.name, err)
			}
		}
		if column.err != nil {
			return fmt.Errorf("generated row reference %s.%s: %w", view.Name, column.name, column.err)
		}
	}
	return nil
}

func (a *generatedRowAuthority) validateDependencies() error {
	if a.resolver == nil || a.plan == nil {
		return nil
	}
	for _, view := range a.views {
		if err := a.walkColumns(view, func(dependency string) error {
			// walkColumns already supplies canonical dependency identities.
			// Preserve linked catalog authority, otherwise require one SQL owner.
			shape, err := a.resolver.ResolveShape(dependency)
			if err != nil {
				return err
			}
			if shape != nil && shape.Descriptor != nil {
				return nil
			}
			switch len(a.owners[dependency]) {
			case 1:
				return nil
			case 0:
				return fmt.Errorf("type %q was not found", dependency)
			default:
				return fmt.Errorf("generated type %q has multiple SQL owners", dependency)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *generatedRowAuthority) validateCycles() error {
	if a.plan == nil {
		return nil
	}
	state := map[*spec.View]int{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil {
			return nil
		}
		if state[view] == 1 {
			return fmt.Errorf("generated named row dependency cycle at %q", view.TypeName)
		}
		if state[view] == 2 {
			return nil
		}
		state[view] = 1
		// A scalar reference needs unique name authority. An ordinary relation
		// already points at its concrete view; repeated names retain the planner's
		// existing compatible-leaf and body/shared-Current admission policy.
		if err := a.walkColumns(view, func(key string) error {
			owners := a.owners[key]
			if len(owners) == 0 {
				return nil
			}
			// A selected authored package row keeps its existing Go shape.
			// Its recursive pointers are not dependencies of generated SQL rows.
			// Generated descriptors (including stale ones) still follow SQL owners.
			if a.resolver != nil {
				resolved, err := a.resolver.ResolveWithProvenance(key)
				if err != nil {
					return err
				}
				if resolved != nil && resolved.Origin == typecatalog.TypeOriginPackage {
					return nil
				}
			}
			if len(owners) != 1 {
				return fmt.Errorf("generated type %q has multiple SQL owners", key)
			}
			return visit(owners[0])
		}); err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		state[view] = 2
		return nil
	}
	for _, view := range a.views {
		if err := visit(view); err != nil {
			return err
		}
	}
	return nil
}
