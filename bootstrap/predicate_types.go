package bootstrap

import (
	"fmt"
	"reflect"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xunsafe"
)

// linkPredicateTypes only follows referenced types. It never reads source,
// discovers component holders, loads resources, or replaces existing authority.
func linkPredicateTypes(catalog *typecatalog.Catalog, component *spec.Component, context *typecatalog.ResolutionContext) (*typecatalog.Catalog, error) {
	names, err := readerpredicate.References(component, context)
	if err != nil || len(names) == 0 {
		return catalog, err
	}
	if catalog == nil {
		catalog = typecatalog.NewCatalog()
	} else {
		catalog, err = catalog.Clone()
		if err != nil {
			return nil, err
		}
	}
	packages := map[string][]reflect.Type{}
	for _, name := range names {
		// Source-backed and explicitly registered declarations retain authority,
		// including source-only declarations that must fail strict validation.
		_, found, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, name)
		if err != nil {
			return nil, err
		}
		if found {
			continue
		}
		path, typeName, err := (xshape.Resolver{}).CanonicalReference(name)
		if err != nil {
			return nil, err
		}
		if path == "" {
			continue
		}
		candidates, loaded := packages[path]
		if !loaded {
			candidates = xunsafe.PackageTypes(path)
			packages[path] = candidates
		}
		var selected reflect.Type
		for _, candidate := range candidates {
			candidate = dereference(candidate)
			if candidate == nil || candidate.PkgPath() != path || candidate.Name() != typeName {
				continue
			}
			if selected != nil && selected != candidate {
				return nil, fmt.Errorf("predicate type %q has ambiguous linked identities", name)
			}
			selected = candidate
		}
		if selected != nil {
			if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, x.NewType(selected)); err != nil {
				return nil, err
			}
		}
	}
	if err := (readerpredicate.DefinitionCompiler{Context: context, Catalog: catalog, RequireAvailable: true}).Compile(component); err != nil {
		return nil, err
	}
	return catalog, nil
}
