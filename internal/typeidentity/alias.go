// Package typeidentity compares authored Go aliases with linked runtime types.
package typeidentity

import (
	"fmt"
	xshape "github.com/viant/x/shape"
)

// Alias expands only true Go aliases using each declaration's package/import
// scope. Native shape rewriting retains wrappers and named defined types.
// Context preserves the caller's cycle diagnostic wording.
func Alias(expression string, lookup xshape.Lookup, context string) (string, error) {
	return alias(expression, lookup, context, map[string]bool{})
}

func alias(expression string, lookup xshape.Lookup, context string, visiting map[string]bool) (string, error) {
	return (xshape.Resolver{Rewriter: func(name string) (string, error) {
		descriptor, err := lookup(name)
		if err != nil {
			return "", err
		}
		if descriptor == nil || descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil || !descriptor.SynteticType.TypeSpec.Assign.IsValid() {
			return name, nil
		}
		key := descriptor.Key()
		if visiting[key] {
			return "", fmt.Errorf("cyclic %s type alias %q", context, key)
		}
		visiting[key] = true
		defer delete(visiting, key)
		declaration := descriptor.SynteticType
		scope := xshape.Resolver{Package: descriptor.PkgPath, Imports: map[string]string{}}
		for alias, imported := range declaration.Imports {
			if imported != nil {
				scope.Imports[alias] = imported.Path
			}
		}
		target, err := scope.Canonical(declaration.TypeSpec.Type)
		if err != nil {
			return "", err
		}
		return alias(target, lookup, context, visiting)
	}}).Rewrite(expression)
}
