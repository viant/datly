package xgen

import (
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/repository/shape/typectx/source"
	"github.com/viant/x"
	xreflectloader "github.com/viant/x/loader/xreflect"
	"github.com/viant/x/syntetic"
	"github.com/viant/x/syntetic/model"
)

// GenerateFromDQLShape emits Go structs from DQL shape using viant/x registry.
func GenerateFromDQLShape(doc *shape.Document, cfg *Config) (*Result, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("shape xgen: nil document")
	}
	if cfg == nil {
		cfg = &Config{}
	}
	applyDefaults(cfg)
	projectDir, packageDir, err := resolvePaths(cfg.ProjectDir, cfg.PackageDir)
	if err != nil {
		return nil, err
	}
	packageName := resolvePackageName(cfg.PackageName, packageDir)
	packagePath, err := resolvePackagePath(cfg.PackagePath, projectDir, packageDir)
	if err != nil {
		return nil, err
	}
	fileName := cfg.FileName
	if strings.TrimSpace(fileName) == "" {
		fileName = "shapes_gen.go"
	}
	registry := cfg.Registry
	if registry == nil {
		registry = x.NewRegistry()
	}
	views := extractViews(doc.Root)
	routeTypes := extractRouteIO(doc.Root)
	if len(views) == 0 && len(routeTypes) == 0 {
		return nil, fmt.Errorf("shape xgen: no view or route io declarations")
	}
	typeNames := make([]string, 0, len(views)+len(routeTypes))
	registered := map[string]bool{}
	for _, view := range views {
		typeName := viewTypeName(cfg, view)
		if registered[typeName] {
			continue
		}
		registered[typeName] = true
		if err = registerShapeType(registry, packagePath, typeName, buildStructType(view.columns)); err != nil {
			return nil, err
		}
		typeNames = append(typeNames, typeName)
	}
	for _, ioType := range routeTypes {
		typeName := routeTypeName(cfg, ioType)
		if typeName == "" || registered[typeName] {
			continue
		}
		registered[typeName] = true
		if err = registerShapeType(registry, packagePath, typeName, buildStructType(ioType.fields)); err != nil {
			return nil, err
		}
		typeNames = append(typeNames, typeName)
	}
	namespace, err := syntetic.FromRegistry(registry)
	if err != nil {
		return nil, err
	}
	namespace.PkgName = packageName
	namespace.PkgPath = packagePath
	files, err := namespace.BuildFiles(model.RenderOptions{})
	if err != nil {
		return nil, err
	}
	goFile := files[packagePath]
	if goFile == nil {
		return nil, fmt.Errorf("shape xgen: missing generated package file for %s", packagePath)
	}
	source, err := goFile.Render()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(packageDir, 0o755); err != nil {
		return nil, err
	}
	dest := filepath.Join(packageDir, fileName)
	if exists, checkErr := fileExists(dest); checkErr != nil {
		return nil, checkErr
	} else if exists && !cfg.AllowUnsafeRewrite {
		if issues := rewriteSafetyIssues(doc, cfg, projectDir); len(issues) > 0 && (cfg.StrictProvenance == nil || *cfg.StrictProvenance) {
			return nil, fmt.Errorf("shape xgen: rewrite blocked by type provenance safety: %s", strings.Join(issues, "; "))
		}
		merged, mergeErr := mergeGeneratedShapes(dest, []byte(source), typeNames)
		if mergeErr != nil {
			return nil, mergeErr
		}
		source = string(merged)
	}
	if err = writeAtomic(dest, []byte(source), 0o644); err != nil {
		return nil, err
	}
	sort.Strings(typeNames)
	return &Result{
		FilePath:    dest,
		PackagePath: packagePath,
		PackageName: packageName,
		Types:       typeNames,
	}, nil
}

func rewriteSafetyIssues(doc *shape.Document, cfg *Config, projectDir string) []string {
	if doc == nil || len(doc.TypeResolutions) == 0 {
		return nil
	}
	policy := newRewritePolicy(cfg, projectDir)
	srcResolver, _ := source.New(source.Config{
		ProjectDir:         projectDir,
		AllowedSourceRoots: policy.roots,
		UseGoModuleResolve: policy.useModule,
		UseGOPATHFallback:  policy.useGOPATH,
	})
	var issues []string
	for _, resolution := range doc.TypeResolutions {
		if srcResolver != nil && strings.TrimSpace(resolution.Provenance.File) == "" {
			pkg := firstNonEmpty(strings.TrimSpace(resolution.Provenance.Package), packageOfKey(resolution.ResolvedKey))
			name := typeNameFromKey(resolution.ResolvedKey)
			if pkg != "" && name != "" {
				if file, err := srcResolver.ResolveTypeFile(pkg, name); err == nil {
					resolution.Provenance.File = file
					if resolution.Provenance.Kind == "" || strings.EqualFold(resolution.Provenance.Kind, "registry") {
						resolution.Provenance.Kind = "ast_type"
					}
				}
			}
		}
		if issue := resolutionSafetyIssue(resolution, policy); issue != "" {
			issues = append(issues, issue)
		}
	}
	sort.Strings(issues)
	return uniqueStrings(issues)
}

func resolutionSafetyIssue(resolution typectx.Resolution, policy rewritePolicy) string {
	kind := strings.TrimSpace(strings.ToLower(resolution.Provenance.Kind))
	if kind == "" {
		kind = "registry"
	}
	if !policy.allowedKinds[kind] {
		return fmt.Sprintf("expression=%q kind=%q", resolution.Expression, resolution.Provenance.Kind)
	}

	sourceFile := strings.TrimSpace(resolution.Provenance.File)
	if sourceFile == "" {
		return ""
	}
	if !filepath.IsAbs(sourceFile) {
		sourceFile = filepath.Clean(filepath.Join(policy.projectDir, sourceFile))
	}
	if safe, err := source.IsWithinAnyRoot(sourceFile, policy.roots); err != nil || !safe {
		return fmt.Sprintf("expression=%q source=%q outside_trusted_roots", resolution.Expression, resolution.Provenance.File)
	}
	return ""
}

type rewritePolicy struct {
	projectDir   string
	allowedKinds map[string]bool
	roots        []string
	useModule    bool
	useGOPATH    bool
}

func newRewritePolicy(cfg *Config, projectDir string) rewritePolicy {
	allowedKinds := map[string]bool{
		"builtin":       true,
		"resource_type": true,
		"ast_type":      true,
	}
	if len(cfg.AllowedProvenanceKinds) > 0 {
		allowedKinds = map[string]bool{}
		for _, item := range cfg.AllowedProvenanceKinds {
			item = strings.TrimSpace(strings.ToLower(item))
			if item != "" {
				allowedKinds[item] = true
			}
		}
	}
	useModule := true
	if cfg.UseGoModuleResolve != nil {
		useModule = *cfg.UseGoModuleResolve
	}
	useGOPATH := true
	if cfg.UseGOPATHFallback != nil {
		useGOPATH = *cfg.UseGOPATHFallback
	}
	return rewritePolicy{
		projectDir:   projectDir,
		allowedKinds: allowedKinds,
		roots:        source.NormalizeRoots(projectDir, cfg.AllowedSourceRoots),
		useModule:    useModule,
		useGOPATH:    useGOPATH,
	}
}

func typeNameFromKey(key string) string {
	index := strings.LastIndex(key, ".")
	if index == -1 || index+1 >= len(key) {
		return ""
	}
	return key[index+1:]
}

func packageOfKey(key string) string {
	index := strings.LastIndex(key, ".")
	if index == -1 {
		return ""
	}
	return key[:index]
}

func uniqueStrings(items []string) []string {
	if len(items) < 2 {
		return items
	}
	result := items[:0]
	var previous string
	for i, item := range items {
		if i == 0 || item != previous {
			result = append(result, item)
		}
		previous = item
	}
	return result
}

func registerShapeType(registry *x.Registry, packagePath string, typeName string, rType reflect.Type) error {
	st, err := xreflectloader.BuildType(rType,
		xreflectloader.WithPackagePath(packagePath),
		xreflectloader.WithNamePolicy(func(reflect.Type) (string, bool) {
			return typeName, false
		}))
	if err != nil {
		return fmt.Errorf("shape xgen: build type %s failed: %w", typeName, err)
	}
	st.Name = typeName
	st.PkgPath = packagePath
	if st.TypeSpec != nil {
		st.TypeSpec.Name = ast.NewIdent(typeName)
	}
	registry.Register(x.NewType(rType,
		x.WithName(typeName),
		x.WithPkgPath(packagePath),
		x.WithSyntheticType(st)))
	return nil
}

type viewDescriptor struct {
	name       any
	schemaName any
	columns    []columnDescriptor
}

type ioTypeKind string

const (
	ioTypeInput  ioTypeKind = "input"
	ioTypeOutput ioTypeKind = "output"
)

type routeIODescriptor struct {
	kind      ioTypeKind
	routeName string
	routeURI  string
	routeRef  string
	typeName  string
	fields    []columnDescriptor
}

type columnDescriptor struct {
	name     string
	dataType string
}

func extractViews(root map[string]any) []viewDescriptor {
	resource := asMap(root["Resource"])
	if resource == nil {
		return nil
	}
	items := asSlice(resource["Views"])
	result := make([]viewDescriptor, 0, len(items))
	for _, item := range items {
		view := asMap(item)
		if view == nil {
			continue
		}
		schema := asMap(view["Schema"])
		descriptor := viewDescriptor{
			name:       view["Name"],
			schemaName: nil,
		}
		if schema != nil {
			descriptor.schemaName = schema["Name"]
		}
		descriptor.columns = extractColumns(view)
		result = append(result, descriptor)
	}
	return result
}

func extractColumns(view map[string]any) []columnDescriptor {
	var result []columnDescriptor
	if columns := asSlice(view["Columns"]); len(columns) > 0 {
		for _, item := range columns {
			column := asMap(item)
			if column == nil {
				continue
			}
			name := firstNonEmpty(asString(column["Name"]), asString(column["Column"]))
			if name == "" {
				continue
			}
			result = append(result, columnDescriptor{name: name, dataType: asString(column["DataType"])})
		}
	}
	if cfg := asMap(view["ColumnsConfig"]); len(cfg) > 0 {
		keys := make([]string, 0, len(cfg))
		for k := range cfg {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			item := asMap(cfg[key])
			if item == nil {
				item = map[string]any{}
			}
			name := firstNonEmpty(asString(item["Name"]), key)
			result = append(result, columnDescriptor{name: name, dataType: asString(item["DataType"])})
		}
	}
	if len(result) == 0 {
		result = append(result, columnDescriptor{name: "ID", dataType: "int"})
	}
	return result
}

func extractRouteIO(root map[string]any) []routeIODescriptor {
	var result []routeIODescriptor
	for _, item := range asSlice(root["Routes"]) {
		route := asMap(item)
		if route == nil {
			continue
		}
		meta := routeIODescriptor{
			routeName: asString(route["Name"]),
			routeURI:  asString(route["URI"]),
		}
		if routeView := asMap(route["View"]); routeView != nil {
			meta.routeRef = asString(routeView["Ref"])
		}
		if input := asMap(route["Input"]); input != nil {
			entry := meta
			entry.kind = ioTypeInput
			entry.typeName = nestedTypeName(input)
			entry.fields = extractIOFields(input)
			result = append(result, entry)
		}
		if output := asMap(route["Output"]); output != nil {
			entry := meta
			entry.kind = ioTypeOutput
			entry.typeName = nestedTypeName(output)
			entry.fields = extractIOFields(output)
			result = append(result, entry)
		}
	}
	return result
}

func nestedTypeName(io map[string]any) string {
	aType := asMap(io["Type"])
	if aType == nil {
		return ""
	}
	return asString(aType["Name"])
}

func extractIOFields(io map[string]any) []columnDescriptor {
	parameters := asSlice(io["Parameters"])
	if len(parameters) == 0 {
		if t := asMap(io["Type"]); t != nil {
			parameters = asSlice(t["Parameters"])
		}
	}
	fields := make([]columnDescriptor, 0, len(parameters))
	for _, item := range parameters {
		param := asMap(item)
		if param == nil {
			continue
		}
		name := asString(param["Name"])
		if name == "" {
			continue
		}
		dataType := ""
		if schema := asMap(param["Schema"]); schema != nil {
			dataType = asString(schema["DataType"])
		}
		fields = append(fields, columnDescriptor{name: name, dataType: dataType})
	}
	if len(fields) == 0 {
		fields = append(fields, columnDescriptor{name: "ID", dataType: "int"})
	}
	return fields
}

func buildStructType(columns []columnDescriptor) reflect.Type {
	if len(columns) == 0 {
		columns = []columnDescriptor{{name: "ID", dataType: "int"}}
	}
	fields := make([]reflect.StructField, 0, len(columns))
	used := map[string]int{}
	for _, column := range columns {
		fieldName := exportedName(column.name)
		if fieldName == "" {
			fieldName = "Field"
		}
		if count := used[fieldName]; count > 0 {
			fieldName = fmt.Sprintf("%s%d", fieldName, count+1)
		}
		used[fieldName]++
		fields = append(fields, reflect.StructField{
			Name: fieldName,
			Type: parseType(column.dataType),
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s,omitempty" sqlx:"%s"`, strings.ToLower(fieldName), column.name)),
		})
	}
	return reflect.StructOf(fields)
}

func parseType(dataType string) reflect.Type {
	dataType = strings.TrimSpace(dataType)
	if dataType == "" {
		return reflect.TypeOf("")
	}
	switch {
	case strings.HasPrefix(dataType, "[]"):
		return reflect.SliceOf(parseType(strings.TrimPrefix(dataType, "[]")))
	case strings.HasPrefix(dataType, "*"):
		return reflect.PointerTo(parseType(strings.TrimPrefix(dataType, "*")))
	}
	lowered := strings.ToLower(dataType)
	switch lowered {
	case "string", "varchar", "text":
		return reflect.TypeOf("")
	case "bool", "boolean":
		return reflect.TypeOf(true)
	case "int", "integer":
		return reflect.TypeOf(int(0))
	case "int64", "bigint":
		return reflect.TypeOf(int64(0))
	case "int32":
		return reflect.TypeOf(int32(0))
	case "float", "float64", "double", "decimal":
		return reflect.TypeOf(float64(0))
	case "float32":
		return reflect.TypeOf(float32(0))
	case "bytes", "[]byte", "blob":
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf("")
	}
}

func exportedName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var parts []string
	current := strings.Builder{}
	flush := func() {
		if current.Len() == 0 {
			return
		}
		parts = append(parts, current.String())
		current.Reset()
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			current.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	for i, item := range parts {
		if item == strings.ToUpper(item) {
			parts[i] = strings.ToUpper(item[:1]) + strings.ToLower(item[1:])
		} else {
			parts[i] = strings.ToUpper(item[:1]) + item[1:]
		}
	}
	result := strings.Join(parts, "")
	if result == "" {
		return ""
	}
	if result[0] >= '0' && result[0] <= '9' {
		result = "N" + result
	}
	return result
}

func applyDefaults(cfg *Config) {
	if cfg.ViewSuffix == "" {
		cfg.ViewSuffix = "View"
	}
	if cfg.InputSuffix == "" {
		cfg.InputSuffix = "Input"
	}
	if cfg.OutputSuffix == "" {
		cfg.OutputSuffix = "Output"
	}
	if cfg.UseGoModuleResolve == nil {
		value := true
		cfg.UseGoModuleResolve = &value
	}
	if cfg.UseGOPATHFallback == nil {
		value := true
		cfg.UseGOPATHFallback = &value
	}
	if cfg.StrictProvenance == nil {
		value := true
		cfg.StrictProvenance = &value
	}
}

func viewTypeName(cfg *Config, view viewDescriptor) string {
	ctx := ViewTypeContext{
		ViewName:   asString(view.name),
		SchemaName: asString(view.schemaName),
	}
	if cfg.ViewTypeNamer != nil {
		if name := strings.TrimSpace(cfg.ViewTypeNamer(ctx)); name != "" {
			return cfg.TypePrefix + exportedName(name)
		}
	}
	base := firstNonEmpty(ctx.SchemaName, ctx.ViewName)
	if base == "" {
		base = cfg.ViewSuffix
	} else if !hasCaseInsensitiveSuffix(base, cfg.ViewSuffix) {
		base += cfg.ViewSuffix
	}
	return cfg.TypePrefix + exportedName(base)
}

func routeTypeName(cfg *Config, route routeIODescriptor) string {
	ctx := RouteTypeContext{
		RouteName: route.routeName,
		RouteURI:  route.routeURI,
		RouteRef:  route.routeRef,
		TypeName:  route.typeName,
	}
	var custom string
	switch route.kind {
	case ioTypeInput:
		if cfg.InputTypeNamer != nil {
			custom = cfg.InputTypeNamer(ctx)
		}
	case ioTypeOutput:
		if cfg.OutputTypeNamer != nil {
			custom = cfg.OutputTypeNamer(ctx)
		}
	}
	if strings.TrimSpace(custom) != "" {
		return cfg.TypePrefix + exportedName(custom)
	}
	base := firstNonEmpty(ctx.TypeName, ctx.RouteName, ctx.RouteRef, "Route")
	suffix := cfg.OutputSuffix
	if route.kind == ioTypeInput {
		suffix = cfg.InputSuffix
	}
	if !hasCaseInsensitiveSuffix(base, suffix) {
		base += suffix
	}
	return cfg.TypePrefix + exportedName(base)
}

func hasCaseInsensitiveSuffix(value, suffix string) bool {
	if suffix == "" {
		return true
	}
	return strings.HasSuffix(strings.ToLower(value), strings.ToLower(suffix))
}

func firstNonEmpty(values ...string) string {
	for _, item := range values {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}

func asMap(raw any) map[string]any {
	if value, ok := raw.(map[string]any); ok {
		return value
	}
	if value, ok := raw.(map[any]any); ok {
		out := map[string]any{}
		for key, item := range value {
			out[fmt.Sprint(key)] = item
		}
		return out
	}
	return nil
}

func asSlice(raw any) []any {
	if value, ok := raw.([]any); ok {
		return value
	}
	return nil
}

func asString(raw any) string {
	if raw == nil {
		return ""
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fmt.Sprint(raw)
}
