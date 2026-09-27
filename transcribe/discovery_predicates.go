package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func collectPredicatePackages(packages map[string]bool, prepared *dql.PreparedSource, scope string) error {
	if prepared.Err() != nil || prepared.Directives == nil {
		// Let the component compiler report authored diagnostics with source spans.
		return nil
	}
	typeContext := compileTypeContext(&Source{Scope: scope}, prepared.TypeContext)
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, typeContext)
	if err != nil {
		return err
	}
	packagePath := ""
	if typeContext != nil {
		packagePath = typeContext.PackagePath
		if packagePath == "" {
			packagePath = typeContext.DefaultPackage
		}
	}
	for _, param := range prepared.Directives.Params {
		if param == nil {
			continue
		}
		for _, definition := range param.Predicates {
			if definition == nil || !strings.EqualFold(strings.TrimSpace(definition.Name), "handler") || len(definition.Args) != 1 {
				continue
			}
			name, err := resolver.CanonicalDeclaration(definition.Args[0], packagePath)
			if err != nil {
				return fmt.Errorf("predicate type for %s: %w", param.Name, err)
			}
			path, _, err := (xshape.Resolver{}).CanonicalReference(name)
			if err != nil {
				return fmt.Errorf("predicate type for %s: %w", param.Name, err)
			}
			if path != "" {
				packages[path] = true
			}
		}
	}
	return nil
}
