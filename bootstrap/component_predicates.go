package bootstrap

import (
	"fmt"
	"reflect"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func (r *packageComponentResolver) resolveFieldPredicates(contract *packageContract, field xshape.Field, param *spec.Parameter) error {
	if len(param.Predicates) == 0 {
		return nil
	}
	scope, err := r.predicateFieldContext(contract.descriptor, field.Index)
	if err != nil {
		return fmt.Errorf("predicate field %s: %w", field.Name, err)
	}
	return (readerpredicate.DefinitionCompiler{Context: scope}).Compile(&spec.Component{Parameters: []*spec.Parameter{param}})
}

type predicateOwnerKey struct {
	root *x.Type
	path string
}

// Caches belong to one ContractResolver.Resolve call and one immutable type
// authority snapshot. Never publish these private descriptors or contexts.
func (r *packageComponentResolver) predicateFieldContext(root *x.Type, index []int) (*typecatalog.ResolutionContext, error) {
	key := predicateOwnerKey{root: root}
	if len(index) > 1 {
		// The final index identifies the field, not its declaring type.
		key.path = fmt.Sprint(index[:len(index)-1])
	}
	if scope := r.predicateContexts[key]; scope != nil {
		return scope, nil
	}
	var lookup xshape.Lookup
	if r.types != nil {
		lookup = r.predicateDeclaration
	}
	descriptor, err := predicateFieldOwner(root, index, lookup)
	if err != nil {
		return nil, err
	}
	scope := &typecatalog.ResolutionContext{PackagePath: r.component.Key.Scope}
	if descriptor != nil {
		scope.PackagePath = descriptor.PkgPath
		if source := descriptor.SynteticType; source != nil {
			for alias, item := range source.Imports {
				if item != nil {
					scope.Imports = append(scope.Imports, typecatalog.PackageImport{Alias: alias, Package: item.Path})
				}
			}
		} else if authored := r.component.TypeContext; authored != nil && scope.PackagePath == r.component.Key.Scope {
			for _, item := range authored.Imports {
				scope.Imports = append(scope.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
			}
		}
	}
	if r.predicateContexts == nil {
		r.predicateContexts = make(map[predicateOwnerKey]*typecatalog.ResolutionContext)
	}
	r.predicateContexts[key] = scope
	return scope, nil
}

func (r *packageComponentResolver) predicateDeclaration(name string) (*x.Type, error) {
	if descriptor, ok := r.predicateDeclarations[name]; ok {
		return descriptor, nil
	}
	descriptor, err := r.types.Descriptor(name)
	if err != nil {
		return nil, err
	}
	if r.predicateDeclarations == nil {
		r.predicateDeclarations = make(map[string]*x.Type)
	}
	r.predicateDeclarations[name] = descriptor
	return descriptor, nil
}

// Follow the promoted field's index to its declaring type. A holder's imports
// are not the import scope of an external or embedded input declaration.
func predicateFieldOwner(descriptor *x.Type, index []int, lookup xshape.Lookup) (*x.Type, error) {
	for depth := 0; descriptor != nil && depth < 64; depth++ {
		if typ := dereference(descriptor.Type); typ != nil {
			descriptor = x.NewType(typ)
		}
		if lookup != nil {
			// Key memoizes into x.Type; input descriptors may be shared by
			// concurrent compilations. Copy only the header, not the AST.
			identity := *descriptor
			declared, err := lookup(identity.Key())
			if err != nil {
				return nil, err
			}
			if declared != nil {
				descriptor = declared
			}
		}
		if source := descriptor.SynteticType; source != nil && source.TypeSpec != nil {
			resolver := xshape.Resolver{Package: source.PkgPath, Imports: map[string]string{}}
			resolver.Lookup = lookup
			for alias, item := range source.Imports {
				if item != nil {
					resolver.Imports[alias] = item.Path
				}
			}
			// The native resolver owns alias syntax; fields retain their actual
			// declaring import scope and indexes even through promotion.
			canonical, err := resolver.Canonical(source.TypeSpec.Type)
			if err != nil {
				return nil, err
			}
			if _, err := resolver.Reference(canonical); err != nil {
				fields, err := xshape.New(descriptor, lookup).Fields()
				if err != nil {
					return nil, err
				}
				if len(index) <= 1 {
					return descriptor, nil
				}
				canonical = ""
				for _, field := range fields {
					if len(field.Index) == 1 && field.Index[0] == index[0] {
						canonical, err = field.CanonicalType()
						if err != nil {
							return nil, err
						}
						break
					}
				}
				if canonical == "" {
					return nil, fmt.Errorf("invalid embedded field index")
				}
				index = index[1:]
			}
			resolved, err := resolver.Resolve(canonical)
			if err != nil {
				return nil, err
			}
			if resolved == nil || resolved.Descriptor == nil {
				return nil, fmt.Errorf("missing declaring type %s", canonical)
			}
			descriptor = resolved.Descriptor
			continue
		}
		if len(index) <= 1 {
			return descriptor, nil
		}
		typ := dereference(descriptor.Type)
		if typ == nil || typ.Kind() != reflect.Struct || index[0] >= typ.NumField() {
			return nil, fmt.Errorf("invalid embedded field index")
		}
		descriptor = x.NewType(dereference(typ.Field(index[0]).Type))
		index = index[1:]
	}
	return nil, fmt.Errorf("cannot resolve predicate field declaration")
}
