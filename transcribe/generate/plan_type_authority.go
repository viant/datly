package generate

import (
	"fmt"
	"strings"

	xshape "github.com/viant/x/shape"
)

// CanonicalType resolves a generated field expression under the artifact's
// package/import authority. Implicit aliases follow the same generation policy
// used when allocating and emitting these imports; native shape owns syntax.
func (p *Plan) CanonicalType(packagePath, expression string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("generation plan is required")
	}
	resolver := xshape.Resolver{Package: packagePath, Imports: map[string]string{}}
	for _, item := range p.Imports {
		if strings.TrimSpace(item.Package) == "" {
			return "", fmt.Errorf("generated type authority requires a nonempty import package")
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		if alias == "_" {
			continue
		}
		if alias == "." {
			return "", fmt.Errorf("generated type authority cannot resolve a dot import %q", item.Package)
		}
		if prior, ok := resolver.Imports[alias]; ok && prior != item.Package {
			return "", fmt.Errorf("generated import alias %s is ambiguous between %s and %s", alias, prior, item.Package)
		}
		resolver.Imports[alias] = item.Package
	}
	canonical, err := resolver.Canonical(expression)
	if err != nil {
		return "", err
	}
	return p.canonicalAliases(packagePath, canonical)
}

func (p *Plan) canonicalAliases(packagePath, expression string) (string, error) {
	aliases := map[string]string{}
	for _, contract := range []ContractPlan{p.Input, p.Output} {
		if contract.Ownership == ContractGenerated && contract.Package != "" && contract.Package != packagePath {
			aliases[packagePath+"."+contract.Type] = contract.Package + "." + contract.Type
		}
	}
	return (xshape.Resolver{Rewriter: func(name string) (string, error) {
		if replacement := aliases[name]; replacement != "" {
			return replacement, nil
		}
		return name, nil
	}}).Rewrite(expression)
}
