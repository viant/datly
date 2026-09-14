package bootstrap

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func (r *packageComponentResolver) fieldTypeExpression(field xshape.Field) (string, error) {
	if expression := strings.TrimSpace(field.TypeExpr); expression != "" {
		return expression, nil
	}
	if field.ReflectedType == nil {
		return "", fmt.Errorf("field %s has no Go type", field.Name)
	}
	return (xshape.Resolver{Qualifier: func(packagePath, suggested string) string {
		return r.importAlias(packagePath, suggested)
	}}).Expression(field.ReflectedType)
}

func (r *packageComponentResolver) importAlias(packagePath, qualifier string) string {
	packagePath = strings.TrimSpace(packagePath)
	if r.component.TypeContext == nil {
		r.component.TypeContext = &spec.TypeContext{}
	}
	used := map[string]string{}
	for index, item := range r.component.TypeContext.Imports {
		if item.Package == packagePath {
			if item.Alias == "" {
				r.component.TypeContext.Imports[index].Alias = qualifier
				return qualifier
			}
			return item.Alias
		}
		used[item.Alias] = item.Package
	}
	alias := qualifier
	if existing := used[alias]; existing != "" && existing != packagePath {
		for suffix := 2; ; suffix++ {
			candidate := alias + strconv.Itoa(suffix)
			if used[candidate] == "" {
				alias = candidate
				break
			}
		}
	}
	r.component.TypeContext.Imports = append(r.component.TypeContext.Imports, spec.ImportSpec{Alias: alias, Package: packagePath})
	return alias
}
