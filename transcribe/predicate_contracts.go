package transcribe

import (
	"fmt"
	"reflect"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x/shape"
	xpredicate "github.com/viant/xdatly/predicate"
)

// compilePredicateContracts validates generation-time package authority. Source
// contracts may precede generated inputs; executable materialization still
// requires the real compiled handlers through the strict runtime compiler.
func compilePredicateContracts(component *spec.Component, catalog *typecatalog.Catalog, context *typecatalog.ResolutionContext) error {
	references, err := readerpredicate.References(component, context)
	if err != nil {
		return err
	}
	if len(references) != 0 {
		if catalog == nil {
			return fmt.Errorf("compile predicate: type authority is required")
		}
		resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, context)
		if err != nil {
			return err
		}
		for _, name := range references {
			descriptor, found, err := catalog.Resolve(typecatalog.PackageAuthority, name)
			if err != nil {
				return fmt.Errorf("compile predicate %q: %w", name, err)
			}
			if !found || descriptor == nil {
				return fmt.Errorf("compile predicate: handler predicate type %q was not found; add it to a linked package, run datly link sync and rebuild the binary", name)
			}
			if descriptor.Type == nil && (descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil) {
				return fmt.Errorf("compile predicate: handler predicate type %q has no source declaration authority", name)
			}
			implements, err := shape.New(descriptor, resolver.Descriptor).Implements(reflect.TypeFor[xpredicate.Handler](), true)
			if err != nil {
				return fmt.Errorf("compile predicate: inspect handler predicate %q: %w", name, err)
			}
			if !implements {
				return fmt.Errorf("compile predicate: handler predicate type %q does not implement predicate.Handler", name)
			}
		}
	}
	return (readerpredicate.DefinitionCompiler{Context: context}).Compile(component)
}
