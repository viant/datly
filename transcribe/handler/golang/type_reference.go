package golang

import (
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"strings"
)

// typeReference renders canonical semantic field/hook identities using the
// target import table; parsing and rewriting remain native shape operations.
func (l *lowerer) typeReference(ref spec.TypeRef) (ast.Expr, error) {
	name, err := (xshape.Resolver{Package: ref.Package, Imports: l.pathsByAlias}).Canonical(ref.Name)
	if err != nil {
		return nil, err
	}
	if ref.Pointer && !strings.HasPrefix(name, "*") {
		name = "*" + name
	}
	if ref.Cardinality == spec.CardinalityMany && !strings.HasPrefix(name, "[]") {
		name = "[]" + name
	}
	if ref.SlicePointer {
		name = "*" + name
	}
	resolver := xshape.Resolver{Rewriter: func(source string) (string, error) {
		reference, err := (xshape.Resolver{}).Reference(source)
		if err != nil {
			return "", err
		}
		location := reference.Qualifier
		if location == "" || location == l.config.PackagePath {
			return reference.Name, nil
		}
		if existing := l.pathsByAlias[location]; existing != "" && existing != "#generated" {
			location = existing
		}
		alias := l.importsByPath[location]
		if alias == "" {
			alias = l.availableAlias("entitytype")
			l.importsByPath[location] = alias
			l.pathsByAlias[alias] = location
		}
		l.usedImports[location] = true
		return alias + "." + reference.Name, nil
	}}
	rendered, err := resolver.Rewrite(name)
	if err != nil {
		return nil, err
	}
	return parseExpr(rendered), nil
}
