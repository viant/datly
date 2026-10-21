package transcribe

import (
	"fmt"
	"go/types"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/internal/program"
	"github.com/viant/x"
)

// Resolve discovery references with the same selected-build importer that
// proved the factory and contracts. No runtime contract or AST is fabricated.
func sourceHandlerWireTypes(loader types.Importer, fields map[string]sourceHandlerField, tc *spec.TypeContext) ([]*x.Type, func(string, string) (string, bool), error) {
	resolve := func(expression string, owner *types.Package) (*types.Named, error) {
		object, err := sourceHandlerNamedType(loader, owner, tc, expression)
		if err != nil {
			return nil, err
		}
		named, ok := types.Unalias(object.Type()).(*types.Named)
		if !ok || !named.Obj().Exported() || named.TypeArgs().Len() != 0 || named.TypeParams().Len() != 0 {
			return nil, fmt.Errorf("wireType %q requires an exported nongeneric named representation", expression)
		}
		return named, nil
	}
	owners := map[string]*types.Named{}
	descriptors := map[string]*x.Type{}
	for name, field := range fields {
		expression := strings.TrimSpace(field.tags.Get("wireType"))
		if expression == "" {
			continue
		}
		named, err := resolve(expression, field.pkg)
		if err != nil {
			return nil, nil, fmt.Errorf("handler parameter %q wireType: %w", name, err)
		}
		owners[name] = named
		descriptor := &x.Type{PkgPath: named.Obj().Pkg().Path(), Name: named.Obj().Name()}
		descriptors[descriptor.Key()] = descriptor
	}
	keys := make([]string, 0, len(descriptors))
	for key := range descriptors {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*x.Type, 0, len(keys))
	for _, key := range keys {
		result = append(result, descriptors[key])
	}
	match := func(name, expression string) (string, bool) {
		owner := owners[name]
		if owner == nil {
			return "", false
		}
		named, err := resolve(expression, fields[name].pkg)
		return owner.Obj().Pkg().Path() + "." + owner.Obj().Name(), err == nil && types.Identical(owner, named)
	}
	return result, match, nil
}

// sourceHandlerNamedType keeps authored and persisted leaf lookup on the same
// selected-build importer that owns the factory and contracts.
func sourceHandlerNamedType(loader types.Importer, owner *types.Package, tc *spec.TypeContext, expression string) (*types.TypeName, error) {
	expression = strings.TrimSpace(expression)
	pkg, name := "", expression
	if strings.Contains(expression, ".") {
		var err error
		pkg, name, err = program.Symbol(expression)
		if err != nil {
			return nil, err
		}
		if tc != nil {
			for _, imp := range tc.Imports {
				if pkg == imp.Alias {
					pkg = imp.Package
				}
			}
		}
	} else if owner != nil {
		pkg = owner.Path()
	}
	if pkg == "" {
		return nil, fmt.Errorf("type %q requires package authority", expression)
	}
	loaded, err := loader.Import(pkg)
	if err != nil {
		return nil, err
	}
	object, ok := loaded.Scope().Lookup(name).(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("type %q is not a named Go type", expression)
	}
	return object, nil
}
