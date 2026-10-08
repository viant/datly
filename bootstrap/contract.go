package bootstrap

import (
	"bytes"
	"fmt"
	"go/format"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func packageContractExpression(role, generic, tagged string) (string, error) {
	generic = strings.TrimSpace(generic)
	tagged = strings.TrimSpace(tagged)
	if generic == "" {
		return tagged, nil
	}
	if tagged == "" {
		return generic, nil
	}
	equal, err := (xshape.Contract{}).Equivalent(generic, tagged)
	if err != nil {
		return "", fmt.Errorf("compare package %s contract: %w", role, err)
	}
	if !equal {
		return "", fmt.Errorf("component tag %s contract %q conflicts with Component contract %q", role, tagged, generic)
	}
	return generic, nil
}

// ValidateContractTypes verifies that linked types are the exact contracts
// declared by this holder's Component[I,O] arguments.
func (s *RouteSource) ValidateContractTypes(inputType, outputType reflect.Type) error {
	return s.validateContractTypes(inputType, outputType, nil)
}

func (s *RouteSource) validateContractTypes(inputType, outputType reflect.Type, catalog *typecatalog.Resolver) error {
	if s == nil {
		return fmt.Errorf("package route source is required")
	}
	imports := make(map[string]string, len(s.Imports))
	for _, item := range s.Imports {
		imports[strings.TrimSpace(item.Alias)] = strings.TrimSpace(item.Package)
	}
	validator := xshape.Contract{Packages: xshape.Packages{
		Default: strings.TrimSpace(s.PackagePath), Imports: imports,
	}}
	for _, contract := range []struct {
		role       string
		expression string
		actual     reflect.Type
	}{
		{role: "input", expression: s.InputType, actual: inputType},
		{role: "output", expression: s.OutputType, actual: outputType},
	} {
		if contract.actual == nil {
			return fmt.Errorf("linked %s type is required", contract.role)
		}
		if err := validator.ValidateLinked(contract.expression, contract.actual); err != nil {
			// Reflection erases Go aliases. Source-backed indexing has the
			// declarations needed to prove their identity; linked-only callers
			// retain the existing strict comparison without source discovery.
			if catalog != nil {
				expression, resolveErr := resolveContractAliases(contract.expression, s.PackagePath, imports, catalog, map[string]bool{})
				if resolveErr != nil {
					return fmt.Errorf("resolve %s contract aliases: %w", contract.role, resolveErr)
				}
				if aliasErr := (xshape.Contract{}).ValidateLinked(expression, contract.actual); aliasErr == nil {
					continue
				}
			}
			return fmt.Errorf("linked %s type %s does not match holder contract %q: %w", contract.role, contract.actual, contract.expression, err)
		}
	}
	return nil
}

// Expand only '=' declarations, never the underlying shape of a distinct Go
// named type. Each alias uses its declaring file's package/import context.
func resolveContractAliases(expression, pkg string, imports map[string]string, catalog *typecatalog.Resolver, active map[string]bool) (string, error) {
	context := xshape.Resolver{Package: pkg, Imports: imports}
	return (xshape.Resolver{Rewriter: func(name string) (string, error) {
		identity, err := context.Canonical(name)
		if err != nil {
			return "", err
		}
		descriptor, err := catalog.Descriptor(identity)
		if err != nil {
			return "", err
		}
		if descriptor == nil || descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil || !descriptor.SynteticType.TypeSpec.Assign.IsValid() {
			return identity, nil
		}
		if active[identity] {
			return "", fmt.Errorf("cyclic contract alias %s", identity)
		}
		active[identity] = true
		defer delete(active, identity)
		declaration := descriptor.SynteticType
		var source bytes.Buffer
		if err := format.Node(&source, token.NewFileSet(), declaration.TypeSpec.Type); err != nil {
			return "", err
		}
		aliases := map[string]string{}
		for alias, imported := range declaration.Imports {
			if imported != nil {
				aliases[alias] = imported.Path
			}
		}
		return resolveContractAliases(source.String(), descriptor.PkgPath, aliases, catalog, active)
	}}).Rewrite(expression)
}
