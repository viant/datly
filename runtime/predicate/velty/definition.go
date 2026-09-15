package velty

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// DefinitionCompiler preserves handler type identity before metadata leaves its
// authored import scope. It does not require linked types during generation.
// Builtin predicate arguments remain strings owned by their templates.
type DefinitionCompiler struct {
	Context *typecatalog.ResolutionContext
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
			// Arity and linked Handler signature validation remain in Compile.
			if !entry.handler || len(definition.Args) != 1 || strings.TrimSpace(definition.Args[0]) == "" {
				continue
			}
			name, err := types.CanonicalDeclaration(definition.Args[0], packagePath)
			if err != nil {
				return fmt.Errorf("compile predicate %s for %s: %w", definition.Name, param.Name, err)
			}
			definition.Args[0] = name
		}
	}
	return nil
}
