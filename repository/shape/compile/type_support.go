package compile

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/x"
	"github.com/viant/xunsafe"
)

func applyLinkedTypeSupport(result *plan.Result, source *shape.Source) {
	if result == nil || source == nil {
		return
	}
	registry := source.EnsureTypeRegistry()
	resolver := typectx.NewResolver(registry, result.TypeContext)
	rootTypeKey := resolveRootTypeKey(source, resolver, registry)
	existing := existingTypesByName(result.Types)

	for idx, item := range result.Views {
		if item == nil {
			continue
		}
		resolvedType := resolveViewType(item, idx == 0, rootTypeKey, resolver, registry, result.TypeContext, source)
		if resolvedType == nil || resolvedType.Type == nil {
			continue
		}
		rType := unwrapResolvedType(resolvedType.Type)
		if rType == nil {
			continue
		}
		item.ElementType = rType
		if strings.EqualFold(strings.TrimSpace(item.Cardinality), "many") {
			item.FieldType = reflect.SliceOf(rType)
		} else {
			item.FieldType = rType
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

func resolveViewType(item *plan.View, root bool, rootTypeKey string, resolver *typectx.Resolver, registry *x.Registry, ctx *typectx.Context, source *shape.Source) *x.Type {
	for _, candidate := range viewTypeCandidates(item, root, rootTypeKey) {
		if key := resolveTypeKey(candidate, resolver, registry); key != "" {
			if registry != nil {
				if resolved := registry.Lookup(key); resolved != nil && resolved.Type != nil {
					return resolved
				}
			}
		}
		if linked := lookupLinkedType(candidate, ctx, source); linked != nil {
			return x.NewType(linked)
		}
	}
	return nil
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

func viewTypeCandidates(item *plan.View, root bool, rootTypeKey string) []string {
	if item == nil {
		return nil
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
	return candidates
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

func lookupLinkedType(typeExpr string, ctx *typectx.Context, source *shape.Source) reflect.Type {
	base := normalizeTypeLookupKey(typeExpr)
	if base == "" {
		return nil
	}
	if pkg, name, ok := splitQualifiedType(base); ok {
		if fullPkg := packagePathForAlias(pkg, ctx); fullPkg != "" {
			if linked := xunsafe.LookupType(fullPkg + "/" + name); linked != nil {
				return linked
			}
			if linked := lookupASTType(fullPkg, name, ctx, source); linked != nil {
				return linked
			}
		}
		return nil
	}
	if ctx != nil && strings.TrimSpace(ctx.PackagePath) != "" {
		if linked := xunsafe.LookupType(strings.TrimSpace(ctx.PackagePath) + "/" + base); linked != nil {
			return linked
		}
		if linked := lookupASTType(strings.TrimSpace(ctx.PackagePath), base, ctx, source); linked != nil {
			return linked
		}
	}
	return nil
}

func splitQualifiedType(value string) (string, string, bool) {
	index := strings.Index(value, ".")
	if index <= 0 || index+1 >= len(value) {
		return "", "", false
	}
	return strings.TrimSpace(value[:index]), strings.TrimSpace(value[index+1:]), true
}

func packagePathForAlias(alias string, ctx *typectx.Context) string {
	alias = strings.TrimSpace(alias)
	if alias == "" || ctx == nil {
		return ""
	}
	for _, item := range ctx.Imports {
		if strings.TrimSpace(item.Alias) == alias {
			return strings.TrimSpace(item.Package)
		}
	}
	if strings.TrimSpace(ctx.PackageName) == alias {
		return strings.TrimSpace(ctx.PackagePath)
	}
	return ""
}

func lookupASTType(pkgPath, typeName string, ctx *typectx.Context, source *shape.Source) reflect.Type {
	pkgDir := resolveTypePackageDir(pkgPath, ctx, source)
	if pkgDir == "" {
		return nil
	}
	return parseNamedStructType(pkgDir, typeName)
}

func resolveTypePackageDir(pkgPath string, ctx *typectx.Context, source *shape.Source) string {
	if ctx == nil {
		return ""
	}
	moduleRoot := nearestModuleRoot(source)
	if moduleRoot == "" {
		if strings.TrimSpace(ctx.PackagePath) == strings.TrimSpace(pkgPath) {
			if dir := strings.TrimSpace(ctx.PackageDir); dir != "" {
				if filepath.IsAbs(dir) {
					return dir
				}
			}
		}
		return ""
	}
	modulePath := detectModulePath(moduleRoot)
	if modulePath != "" {
		if rel, ok := packagePathRelative(modulePath, pkgPath); ok {
			if rel == "" {
				return moduleRoot
			}
			return filepath.Join(moduleRoot, filepath.FromSlash(rel))
		}
	}
	if strings.TrimSpace(ctx.PackagePath) == strings.TrimSpace(pkgPath) {
		if dir := strings.TrimSpace(ctx.PackageDir); dir != "" {
			if filepath.IsAbs(dir) {
				return dir
			}
			return filepath.Join(moduleRoot, filepath.FromSlash(dir))
		}
	}
	return ""
}

func packageNameForPath(pkgPath string, ctx *typectx.Context) string {
	if ctx != nil && strings.TrimSpace(ctx.PackagePath) == strings.TrimSpace(pkgPath) && strings.TrimSpace(ctx.PackageName) != "" {
		return strings.TrimSpace(ctx.PackageName)
	}
	if index := strings.LastIndex(strings.TrimSpace(pkgPath), "/"); index != -1 {
		return strings.TrimSpace(pkgPath[index+1:])
	}
	return strings.TrimSpace(pkgPath)
}

func nearestModuleRoot(source *shape.Source) string {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	current := filepath.Dir(strings.TrimSpace(source.Path))
	for current != "" && current != string(filepath.Separator) && current != "." {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return ""
}

func parseNamedStructType(pkgDir, typeName string) reflect.Type {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, pkgDir, nil, parser.ParseComments)
	if err != nil || len(pkgs) == 0 {
		return nil
	}
	specs := map[string]*ast.TypeSpec{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.TYPE {
					continue
				}
				for _, spec := range gen.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name == nil {
						continue
					}
					specs[typeSpec.Name.Name] = typeSpec
				}
			}
		}
	}
	cache := map[string]reflect.Type{}
	inProgress := map[string]bool{}
	var buildNamed func(name string) reflect.Type
	var buildExpr func(expr ast.Expr) reflect.Type

	buildNamed = func(name string) reflect.Type {
		if cached, ok := cache[name]; ok {
			return cached
		}
		if inProgress[name] {
			return reflect.TypeOf(new(interface{})).Elem()
		}
		spec := specs[name]
		if spec == nil {
			return nil
		}
		inProgress[name] = true
		rType := buildExpr(spec.Type)
		delete(inProgress, name)
		if rType != nil {
			cache[name] = rType
		}
		return rType
	}

	buildExpr = func(expr ast.Expr) reflect.Type {
		switch actual := expr.(type) {
		case *ast.Ident:
			switch actual.Name {
			case "string":
				return reflect.TypeOf("")
			case "bool":
				return reflect.TypeOf(true)
			case "int":
				return reflect.TypeOf(int(0))
			case "int8":
				return reflect.TypeOf(int8(0))
			case "int16":
				return reflect.TypeOf(int16(0))
			case "int32":
				return reflect.TypeOf(int32(0))
			case "int64":
				return reflect.TypeOf(int64(0))
			case "uint":
				return reflect.TypeOf(uint(0))
			case "uint8":
				return reflect.TypeOf(uint8(0))
			case "uint16":
				return reflect.TypeOf(uint16(0))
			case "uint32":
				return reflect.TypeOf(uint32(0))
			case "uint64":
				return reflect.TypeOf(uint64(0))
			case "float32":
				return reflect.TypeOf(float32(0))
			case "float64":
				return reflect.TypeOf(float64(0))
			case "interface{}", "any":
				return reflect.TypeOf(new(interface{})).Elem()
			default:
				return buildNamed(actual.Name)
			}
		case *ast.StarExpr:
			if inner := buildExpr(actual.X); inner != nil {
				return reflect.PtrTo(inner)
			}
		case *ast.ArrayType:
			if actual.Len == nil {
				if inner := buildExpr(actual.Elt); inner != nil {
					return reflect.SliceOf(inner)
				}
			}
		case *ast.MapType:
			key := buildExpr(actual.Key)
			value := buildExpr(actual.Value)
			if key != nil && value != nil {
				return reflect.MapOf(key, value)
			}
		case *ast.InterfaceType:
			return reflect.TypeOf(new(interface{})).Elem()
		case *ast.SelectorExpr:
			if ident, ok := actual.X.(*ast.Ident); ok {
				if ident.Name == "time" && actual.Sel != nil && actual.Sel.Name == "Time" {
					return reflect.TypeOf(time.Time{})
				}
			}
		case *ast.StructType:
			fields := make([]reflect.StructField, 0, len(actual.Fields.List))
			seen := map[string]bool{}
			for _, field := range actual.Fields.List {
				if field == nil {
					continue
				}
				fieldType := buildExpr(field.Type)
				if fieldType == nil {
					continue
				}
				tag := reflect.StructTag("")
				if field.Tag != nil {
					tag = reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
				}
				if len(field.Names) == 0 {
					if name := exportedEmbeddedFieldName(field.Type); name != "" {
						if seen[name] {
							continue
						}
						seen[name] = true
						fields = append(fields, reflect.StructField{Name: name, Type: fieldType, Tag: tag, Anonymous: true})
					}
					continue
				}
				for _, name := range field.Names {
					if name == nil || !name.IsExported() {
						continue
					}
					if seen[name.Name] {
						continue
					}
					seen[name.Name] = true
					fields = append(fields, reflect.StructField{Name: name.Name, Type: fieldType, Tag: tag})
				}
			}
			if len(fields) > 0 {
				return reflect.StructOf(fields)
			}
		}
		return nil
	}

	return buildNamed(typeName)
}

func exportedEmbeddedFieldName(expr ast.Expr) string {
	switch actual := expr.(type) {
	case *ast.Ident:
		if actual.IsExported() {
			return actual.Name
		}
	case *ast.SelectorExpr:
		if actual.Sel != nil && actual.Sel.IsExported() {
			return actual.Sel.Name
		}
	case *ast.StarExpr:
		return exportedEmbeddedFieldName(actual.X)
	}
	return ""
}
