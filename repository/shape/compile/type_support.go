package compile

import (
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/x"
)

func applyLinkedTypeSupport(result *plan.Result, source *shape.Source) {
	if result == nil || source == nil {
		return
	}
	registry := source.EnsureTypeRegistry()
	if registry == nil || len(registry.Keys()) == 0 {
		return
	}
	resolver := typectx.NewResolver(registry, result.TypeContext)
	rootTypeKey := resolveRootTypeKey(source, resolver, registry)
	existing := existingTypesByName(result.Types)

	for idx, item := range result.Views {
		if item == nil {
			continue
		}
		resolvedKey := resolveViewTypeKey(item, idx == 0, rootTypeKey, resolver, registry)
		if resolvedKey == "" {
			continue
		}
		resolvedType := registry.Lookup(resolvedKey)
		if resolvedType == nil || resolvedType.Type == nil {
			continue
		}
		rType := unwrapResolvedType(resolvedType.Type)
		if rType == nil {
			continue
		}
		typeExpr, typePkg := schemaTypeExpression(rType, result.TypeContext)
		if shouldSetSchemaType(item) && typeExpr != "" {
			item.SchemaType = typeExpr
		}
		name := strings.TrimSpace(rType.Name())
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if existing[key] {
			continue
		}
		result.Types = append(result.Types, &plan.Type{
			Name:        name,
			DataType:    typeExpr,
			Cardinality: strings.TrimSpace(item.Cardinality),
			Package:     typePkg,
			ModulePath:  strings.TrimSpace(rType.PkgPath()),
		})
		existing[key] = true
	}
}

func resolveRootTypeKey(source *shape.Source, resolver *typectx.Resolver, registry *x.Registry) string {
	if source == nil || registry == nil {
		return ""
	}
	if key := resolveTypeKey(strings.TrimSpace(source.TypeName), resolver, registry); key != "" {
		return key
	}
	rType, err := source.ResolveRootType()
	if err != nil || rType == nil {
		return ""
	}
	return resolveTypeKey(x.NewType(rType).Key(), resolver, registry)
}

func resolveViewTypeKey(item *plan.View, root bool, rootTypeKey string, resolver *typectx.Resolver, registry *x.Registry) string {
	if item == nil || registry == nil {
		return ""
	}
	candidates := make([]string, 0, 8)
	seen := map[string]bool{}
	appendCandidate := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if seen[value] {
			return
		}
		seen[value] = true
		candidates = append(candidates, value)
	}

	if root && rootTypeKey != "" {
		appendCandidate(rootTypeKey)
	}
	if item.Declaration != nil {
		appendCandidate(item.Declaration.DataType)
		appendCandidate(item.Declaration.Of)
	}
	appendCandidate(item.SchemaType)
	name := toExportedTypeName(item.Name)
	if name != "" {
		appendCandidate(name + "View")
		appendCandidate(name)
	}
	for _, candidate := range candidates {
		if key := resolveTypeKey(candidate, resolver, registry); key != "" {
			return key
		}
	}
	return ""
}

func resolveTypeKey(typeExpr string, resolver *typectx.Resolver, registry *x.Registry) string {
	if registry == nil {
		return ""
	}
	base := normalizeTypeLookupKey(typeExpr)
	if base == "" {
		return ""
	}
	if registry.Lookup(base) != nil {
		return base
	}
	if resolver == nil {
		return ""
	}
	resolved, err := resolver.Resolve(base)
	if err != nil || resolved == "" {
		return ""
	}
	if registry.Lookup(resolved) == nil {
		return ""
	}
	return resolved
}

func normalizeTypeLookupKey(typeExpr string) string {
	value := strings.TrimSpace(typeExpr)
	for {
		switch {
		case strings.HasPrefix(value, "*"):
			value = strings.TrimPrefix(value, "*")
		case strings.HasPrefix(value, "[]"):
			value = strings.TrimPrefix(value, "[]")
		default:
			return strings.TrimSpace(value)
		}
	}
}

func shouldSetSchemaType(item *plan.View) bool {
	if item == nil {
		return false
	}
	current := strings.TrimSpace(item.SchemaType)
	if current == "" {
		return true
	}
	expectedDefault := "*" + toExportedTypeName(item.Name) + "View"
	return current == expectedDefault
}

func existingTypesByName(input []*plan.Type) map[string]bool {
	result := map[string]bool{}
	for _, item := range input {
		if item == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(item.Name))
		if name == "" {
			continue
		}
		result[name] = true
	}
	return result
}

func schemaTypeExpression(rType reflect.Type, ctx *typectx.Context) (string, string) {
	rType = unwrapResolvedType(rType)
	if rType == nil {
		return "", ""
	}
	typeName := strings.TrimSpace(rType.Name())
	if typeName == "" {
		return "", ""
	}
	pkgPath := strings.TrimSpace(rType.PkgPath())
	if pkgPath == "" {
		return "*" + typeName, ""
	}
	pkgAlias := packageAlias(pkgPath, ctx)
	if pkgAlias == "" {
		return "*" + typeName, ""
	}
	return "*" + pkgAlias + "." + typeName, pkgAlias
}

func packageAlias(pkgPath string, ctx *typectx.Context) string {
	pkgPath = strings.TrimSpace(pkgPath)
	if pkgPath == "" {
		return ""
	}
	if ctx != nil {
		for _, item := range ctx.Imports {
			if strings.TrimSpace(item.Package) != pkgPath {
				continue
			}
			alias := strings.TrimSpace(item.Alias)
			if alias != "" {
				return alias
			}
		}
		if strings.TrimSpace(ctx.PackagePath) == pkgPath && strings.TrimSpace(ctx.PackageName) != "" {
			return strings.TrimSpace(ctx.PackageName)
		}
	}
	index := strings.LastIndex(pkgPath, "/")
	if index == -1 || index+1 >= len(pkgPath) {
		return pkgPath
	}
	return pkgPath[index+1:]
}

func unwrapResolvedType(rType reflect.Type) reflect.Type {
	for rType != nil {
		switch rType.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			rType = rType.Elem()
		default:
			return rType
		}
	}
	return nil
}
