package velty

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x/shape"
	xpredicate "github.com/viant/xdatly/predicate"
)

// DefinitionCompiler preserves handler type identity before metadata leaves its
// authored import scope. Transcription also checks compiled package authority
// so a missing, unlinked, or incompatible predicate cannot become a generated
// component.
// Builtin predicate arguments remain strings owned by their templates.
type DefinitionCompiler struct {
	Context          *typecatalog.ResolutionContext
	Catalog          *typecatalog.Catalog
	RequireAvailable bool
}

// Compile updates compiler-owned component metadata. Callers must clone shared
// components before invoking it.
func (c DefinitionCompiler) Compile(component *spec.Component) error {
	if !hasPredicateMetadata(component) {
		return nil
	}
	types, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, c.Context)
	if err != nil {
		return err
	}
	packagePath := ""
	if scope := typecatalog.NormalizeContext(c.Context); scope != nil {
		packagePath = scope.PackagePath
		if packagePath == "" {
			packagePath = scope.DefaultPackage
		}
	}
	registry := predicateRegistry()
	for _, param := range component.Parameters {
		if param == nil {
			continue
		}
		for _, definition := range param.Predicates {
			if definition == nil {
				continue
			}
			entry := registry[strings.ToLower(strings.TrimSpace(definition.Name))]
			if !entry.handler {
				continue
			}
			if len(definition.Args) == 0 || strings.TrimSpace(definition.Args[0]) == "" {
				return fmt.Errorf("compile predicate %s for %s: handler predicate requires an absolute type name", definition.Name, param.Name)
			}
			if len(definition.Args) != 1 {
				return fmt.Errorf("compile predicate %s for %s: handler predicate accepts one type argument", definition.Name, param.Name)
			}
			name, err := types.CanonicalDeclaration(definition.Args[0], packagePath)
			if err != nil {
				return fmt.Errorf("compile predicate %s for %s: %w", definition.Name, param.Name, err)
			}
			if c.RequireAvailable {
				if c.Catalog == nil {
					return fmt.Errorf("compile predicate %s for %s: type authority is required", definition.Name, param.Name)
				}
				descriptor, found, resolveErr := c.Catalog.Resolve(typecatalog.PackageAuthority, name)
				if resolveErr != nil {
					return fmt.Errorf("compile predicate %s for %s: %w", definition.Name, param.Name, resolveErr)
				}
				if !found || descriptor == nil {
					return fmt.Errorf("compile predicate %s for %s: handler predicate type %q was not found; add it to a linked package, run datly link sync and rebuild the binary", definition.Name, param.Name, name)
				}
				if descriptor.Type == nil {
					return fmt.Errorf("compile predicate %s for %s: handler predicate type %q is present in source but not linked into this binary; run datly link sync and rebuild the binary", definition.Name, param.Name, name)
				}
				implements, contractErr := shape.New(descriptor, nil).Implements(reflect.TypeFor[xpredicate.Handler](), true)
				if contractErr != nil {
					return fmt.Errorf("compile predicate %s for %s: inspect handler predicate %q: %w", definition.Name, param.Name, name, contractErr)
				}
				if !implements {
					return fmt.Errorf("compile predicate %s for %s: handler predicate type %q does not implement predicate.Handler", definition.Name, param.Name, name)
				}
			}
			definition.Args[0] = name
		}
	}
	return nil
}
