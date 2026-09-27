package bootstrap

import (
	"fmt"
	"go/ast"
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
	descriptor, err := predicateFieldOwner(contract.descriptor, field.Index, r.types)
	if err != nil {
		return fmt.Errorf("predicate field %s: %w", field.Name, err)
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
	return (readerpredicate.DefinitionCompiler{Context: scope}).Compile(&spec.Component{Parameters: []*spec.Parameter{param}})
}

// Follow the promoted field's index to its declaring type. A holder's imports
// are not the import scope of an external or embedded input declaration.
func predicateFieldOwner(descriptor *x.Type, index []int, types *typecatalog.Resolver) (*x.Type, error) {
	for depth := 0; descriptor != nil && depth < 64; depth++ {
		if typ := dereference(descriptor.Type); typ != nil {
			descriptor = x.NewType(typ)
		}
		if types != nil {
			declared, err := types.Descriptor(descriptor.Key())
			if err != nil {
				return nil, err
			}
			if declared != nil {
				descriptor = declared
			}
		}
		if source := descriptor.SynteticType; source != nil && source.TypeSpec != nil {
			resolver := xshape.Resolver{Package: source.PkgPath, Imports: map[string]string{}}
			if types != nil {
				resolver.Lookup = types.Descriptor
			}
			for alias, item := range source.Imports {
				if item != nil {
					resolver.Imports[alias] = item.Path
				}
			}
			structure, ok := source.TypeSpec.Type.(*ast.StructType)
			var expression ast.Expr
			if !ok {
				expression = source.TypeSpec.Type
			} else {
				if len(index) <= 1 {
					return descriptor, nil
				}
				position := 0
				for _, field := range structure.Fields.List {
					count := len(field.Names)
					if count == 0 {
						count = 1
					}
					if index[0] >= position && index[0] < position+count {
						expression = field.Type
						break
					}
					position += count
				}
				index = index[1:]
			}
			if expression == nil {
				return nil, fmt.Errorf("invalid embedded field index")
			}
			canonical, err := resolver.Canonical(expression)
			if err != nil {
				return nil, err
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
