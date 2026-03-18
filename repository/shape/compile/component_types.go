package compile

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"gopkg.in/yaml.v3"
)

type componentVisitState int

const (
	componentVisitIdle componentVisitState = iota
	componentVisitActive
	componentVisitDone
)

func appendComponentTypes(source *shape.Source, result *plan.Result) []*dqlshape.Diagnostic {
	return appendComponentTypesWithLayout(source, result, defaultCompilePathLayout())
}

func appendComponentTypesWithLayout(source *shape.Source, result *plan.Result, layout compilePathLayout) []*dqlshape.Diagnostic {
	if source == nil || result == nil {
		return nil
	}
	_, routesRoot, dqlRoot, ok := sourceRootsWithLayout(source.Path, layout)
	if !ok {
		return nil
	}
	sourceNamespace, _ := dqlToRouteNamespaceWithLayout(source.Path, layout)
	collector := &componentCollector{
		routesRoot:    routesRoot,
		dqlRoot:       dqlRoot,
		layout:        layout,
		visited:       map[string]componentVisitState{},
		outputByRoute: map[string]string{},
		routeByNS:     map[string]string{},
		typesByName:   map[string]*plan.Type{},
		payloadCache:  map[string]routePayloadLookup{},
		reportedDiag:  map[string]bool{},
	}
	for _, stateItem := range result.States {
		if stateItem == nil || state.Kind(strings.ToLower(stateItem.KindString())) != state.KindComponent {
			continue
		}
		ref := stateItem.InName()
		if ref == "" {
			continue
		}
		namespace := resolveComponentNamespaceWithNamespace(ref, source.Path, dqlRoot, sourceNamespace)
		if namespace == "" {
			collector.diags = append(collector.diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeCompRefInvalid,
				Severity: dqlshape.SeverityWarning,
				Message:  "invalid component reference: " + ref,
				Hint:     "use ../component/ref or GET:/v1/api/... route reference",
				Span:     componentRefSpan(source.DQL, ref),
			})
			continue
		}
		if routeKey, ok := collector.resolveRoute(ref, source.Path); ok && stateItem.In != nil {
			stateItem.In.Name = routeKey
		}
		outputType, ok := collector.collect(namespace, componentRefSpan(source.DQL, ref), true)
		if routeKey := collector.routeKey(namespace); routeKey != "" && stateItem.In != nil {
			stateItem.In.Name = routeKey
		}
		if ok && strings.TrimSpace(outputType) != "" {
			if stateItem.Schema == nil {
				stateItem.Schema = &state.Schema{}
			}
			if stateItem.Schema.Type() == nil {
				if lookup, found := collector.payloadCache[strings.ToLower(strings.TrimSpace(namespace))]; found && lookup.outputType != nil {
					stateItem.Schema.SetType(lookup.outputType)
				}
			}
			if strings.TrimSpace(stateItem.Schema.DataType) == "" {
				stateItem.Schema.DataType = strings.TrimSpace(outputType)
			}
			if payload, found := collector.loadRoutePayload(namespace, componentRefSpan(source.DQL, ref)); found {
				if pkg, modulePath := routeOutputPackage(payload, outputType); pkg != "" || modulePath != "" {
					if strings.TrimSpace(stateItem.Schema.Package) == "" {
						stateItem.Schema.Package = pkg
					}
					if strings.TrimSpace(stateItem.Schema.PackagePath) == "" {
						stateItem.Schema.PackagePath = pkg
					}
					if strings.TrimSpace(stateItem.Schema.ModulePath) == "" {
						stateItem.Schema.ModulePath = modulePath
					}
				}
			}
		}
	}

	sort.Strings(collector.typeOrder)
	existing := map[string]bool{}
	reportedCollision := map[string]bool{}
	for _, item := range result.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		existing[strings.ToLower(strings.TrimSpace(item.Name))] = true
	}
	for _, name := range collector.typeOrder {
		keyName := strings.ToLower(strings.TrimSpace(name))
		if existing[keyName] {
			if !reportedCollision[keyName] {
				collector.diags = append(collector.diags, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeCompTypeCollision,
					Severity: dqlshape.SeverityWarning,
					Message:  "component type skipped due to existing type name: " + strings.TrimSpace(name),
					Hint:     "rename colliding type or keep route type as canonical source",
					Span:     relationSpan(source.DQL, 0),
				})
				reportedCollision[keyName] = true
			}
			continue
		}
		item := collector.typesByName[name]
		result.Types = append(result.Types, item)
		existing[keyName] = true
	}
	return collector.diags
}

type componentCollector struct {
	routesRoot    string
	dqlRoot       string
	layout        compilePathLayout
	routeIndex    *RouteIndex
	routeIndexErr error
	visited       map[string]componentVisitState
	outputByRoute map[string]string
	routeByNS     map[string]string
	// typesByName provides O(1) dedup; typeOrder tracks insertion sequence
	// so the final list can be sorted once rather than extracted from the map.
	typesByName  map[string]*plan.Type
	typeOrder    []string
	payloadCache map[string]routePayloadLookup
	reportedDiag map[string]bool
	diags        []*dqlshape.Diagnostic
}

type routePayloadLookup struct {
	payload     *routePayload
	outputType  reflect.Type
	found       bool
	malformed   bool
	malformedAt string
	detail      string
}

func (c *componentCollector) collect(namespace string, span dqlshape.Span, required bool) (string, bool) {
	key := strings.ToLower(strings.TrimSpace(namespace))
	if key == "" {
		return "", false
	}
	switch c.visited[key] {
	case componentVisitDone:
		return c.outputByRoute[key], true
	case componentVisitActive:
		c.diags = append(c.diags, &dqlshape.Diagnostic{
			Code:     dqldiag.CodeCompCycle,
			Severity: dqlshape.SeverityWarning,
			Message:  "component reference cycle detected at " + namespace,
			Hint:     "break cyclic component references",
			Span:     span,
		})
		return "", false
	}
	c.visited[key] = componentVisitActive

	payload, ok := c.loadRoutePayload(namespace, span)
	if !ok {
		c.visited[key] = componentVisitDone
		if required && !c.hasReported("missing:"+key) {
			c.reportedDiag["missing:"+key] = true
			c.diags = append(c.diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeCompRouteMissing,
				Severity: dqlshape.SeverityWarning,
				Message:  "component route YAML not found: " + namespace,
				Hint:     "ensure matching route exists under repo/dev/Datly/routes",
				Span:     span,
			})
		}
		return "", false
	}

	for _, item := range payload.Resource.Types {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		keyName := strings.ToLower(name)
		if _, exists := c.typesByName[keyName]; exists {
			continue
		}
		c.typeOrder = append(c.typeOrder, keyName)
		c.typesByName[keyName] = &plan.Type{
			Name:        name,
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    strings.TrimSpace(item.DataType),
			Cardinality: strings.TrimSpace(item.Cardinality),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		}
	}

	outputType := routeOutputType(payload)
	c.outputByRoute[key] = outputType
	if routeKey := routePayloadKey(payload); routeKey != "" {
		c.routeByNS[key] = routeKey
	}

	for _, param := range payload.Resource.Parameters {
		if !strings.EqualFold(strings.TrimSpace(param.In.Kind), string(state.KindComponent)) {
			continue
		}
		nextNS := resolveComponentNamespaceFromRoute(strings.TrimSpace(param.In.Name), namespace)
		if nextNS == "" {
			c.diags = append(c.diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeCompRefInvalid,
				Severity: dqlshape.SeverityWarning,
				Message:  "invalid nested component reference: " + strings.TrimSpace(param.In.Name),
				Hint:     "use ../component/ref or GET:/v1/api/... route reference",
				Span:     span,
			})
			continue
		}
		c.collect(nextNS, span, true)
	}

	c.visited[key] = componentVisitDone
	return outputType, true
}

func (c *componentCollector) routeKey(namespace string) string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.routeByNS[strings.ToLower(strings.TrimSpace(namespace))])
}

func (c *componentCollector) resolveRoute(ref, currentSource string) (string, bool) {
	index, err := c.lazyRouteIndex()
	if err != nil || index == nil {
		return "", false
	}
	opts := c.layoutCompileOptions()
	return index.Resolve(ref, currentSource, opts...)
}

func (c *componentCollector) lazyRouteIndex() (*RouteIndex, error) {
	if c == nil {
		return nil, nil
	}
	if c.routeIndex != nil || c.routeIndexErr != nil {
		return c.routeIndex, c.routeIndexErr
	}
	if strings.TrimSpace(c.dqlRoot) == "" {
		return nil, nil
	}
	paths, err := collectDQLSources(c.dqlRoot)
	if err != nil {
		c.routeIndexErr = err
		return nil, err
	}
	c.routeIndex, c.routeIndexErr = BuildRouteIndex(paths, c.layoutCompileOptions()...)
	return c.routeIndex, c.routeIndexErr
}

func (c *componentCollector) layoutCompileOptions() []shape.CompileOption {
	if c == nil {
		return nil
	}
	var opts []shape.CompileOption
	if marker := strings.TrimSpace(c.layout.dqlMarker); marker != "" {
		opts = append(opts, shape.WithDQLPathMarker(marker))
	}
	if rel := strings.TrimSpace(c.layout.routesRelative); rel != "" {
		opts = append(opts, shape.WithRoutesRelativePath(rel))
	}
	return opts
}

func sourceRootsWithLayout(sourcePath string, layout compilePathLayout) (platformRoot, routesRoot, dqlRoot string, ok bool) {
	path := filepath.Clean(strings.TrimSpace(sourcePath))
	if path == "" {
		return "", "", "", false
	}
	normalized := filepath.ToSlash(path)
	marker := layout.dqlMarker
	if marker == "" {
		marker = defaultCompilePathLayout().dqlMarker
	}
	idx := strings.Index(normalized, marker)
	if idx == -1 {
		return "", "", "", false
	}
	platformRoot = path[:idx]
	dqlRoot = filepath.Join(platformRoot, filepath.FromSlash(strings.Trim(marker, "/")))
	routesRoot = joinRelativePath(platformRoot, layout.routesRelative)
	return platformRoot, routesRoot, dqlRoot, true
}

func dqlToRouteNamespace(sourcePath string) (string, bool) {
	return dqlToRouteNamespaceWithLayout(sourcePath, defaultCompilePathLayout())
}

func dqlToRouteNamespaceWithLayout(sourcePath string, layout compilePathLayout) (string, bool) {
	path := filepath.Clean(strings.TrimSpace(sourcePath))
	if path == "" {
		return "", false
	}
	normalized := filepath.ToSlash(path)
	marker := layout.dqlMarker
	if marker == "" {
		marker = defaultCompilePathLayout().dqlMarker
	}
	idx := strings.Index(normalized, marker)
	if idx == -1 {
		return "", false
	}
	relative := strings.TrimPrefix(normalized[idx+len(marker):], "/")
	if relative == "" {
		return "", false
	}
	return strings.Trim(strings.TrimSuffix(relative, filepath.Ext(relative)), "/"), true
}

func resolveComponentNamespace(ref, sourcePath, dqlRoot string) string {
	ref = strings.TrimSpace(ref)
	ref = strings.TrimPrefix(ref, "GET:")
	ref = strings.TrimPrefix(ref, "POST:")
	ref = strings.TrimPrefix(ref, "PUT:")
	ref = strings.TrimPrefix(ref, "PATCH:")
	ref = strings.TrimPrefix(ref, "DELETE:")
	ref = strings.TrimPrefix(ref, "OPTIONS:")
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "/v1/api/") {
		return strings.Trim(strings.TrimPrefix(ref, "/v1/api/"), "/")
	}
	if strings.HasPrefix(ref, "v1/api/") {
		return strings.Trim(strings.TrimPrefix(ref, "v1/api/"), "/")
	}
	if strings.HasPrefix(ref, "/") {
		return strings.Trim(ref, "/")
	}
	if dqlRoot == "" || strings.TrimSpace(sourcePath) == "" {
		return ""
	}
	base := filepath.Dir(filepath.Clean(sourcePath))
	target := filepath.Clean(filepath.Join(base, ref))
	rel, err := filepath.Rel(dqlRoot, target)
	if err != nil {
		return ""
	}
	rel = filepath.ToSlash(rel)
	rel = strings.TrimSuffix(rel, filepath.Ext(rel))
	return strings.Trim(rel, "/")
}

func resolveComponentNamespaceWithNamespace(ref, sourcePath, dqlRoot, sourceNamespace string) string {
	if namespace := resolveComponentNamespace(ref, sourcePath, dqlRoot); namespace != "" {
		return namespace
	}
	return resolveComponentNamespaceFromRoute(ref, sourceNamespace)
}

func resolveComponentNamespaceFromRoute(ref, sourceNamespace string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if namespace := resolveComponentNamespace(ref, "", ""); namespace != "" {
		return namespace
	}
	normalizedBase := strings.Trim(strings.TrimSpace(sourceNamespace), "/")
	if normalizedBase == "" {
		return ""
	}
	baseDir := pathDir(normalizedBase)
	target := filepath.ToSlash(filepath.Clean(filepath.Join(baseDir, ref)))
	target = strings.TrimSuffix(target, filepath.Ext(target))
	return strings.Trim(target, "/")
}

func pathDir(path string) string {
	if path == "" {
		return ""
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) <= 1 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], "/")
}

type routePayload struct {
	Resource struct {
		Types []struct {
			Name        string `yaml:"Name"`
			Alias       string `yaml:"Alias"`
			DataType    string `yaml:"DataType"`
			Cardinality string `yaml:"Cardinality"`
			Package     string `yaml:"Package"`
			ModulePath  string `yaml:"ModulePath"`
		} `yaml:"Types"`
		Parameters []struct {
			Name string `yaml:"Name"`
			In   struct {
				Kind string `yaml:"Kind"`
				Name string `yaml:"Name"`
			} `yaml:"In"`
			Schema struct {
				DataType    string `yaml:"DataType"`
				Name        string `yaml:"Name"`
				Package     string `yaml:"Package"`
				Cardinality string `yaml:"Cardinality"`
			} `yaml:"Schema"`
		} `yaml:"Parameters"`
	} `yaml:"Resource"`
	Routes []struct {
		Method  string `yaml:"Method"`
		URI     string `yaml:"URI"`
		Handler struct {
			OutputType string `yaml:"OutputType"`
		} `yaml:"Handler"`
		Output struct {
			Cardinality string `yaml:"Cardinality"`
			Type        struct {
				Name    string `yaml:"Name"`
				Package string `yaml:"Package"`
			} `yaml:"Type"`
		} `yaml:"Output"`
	} `yaml:"Routes"`
}

func readRoutePayload(routesRoot, namespace string) routePayloadLookup {
	candidates := routeYAMLCandidates(routesRoot, namespace)
	lookup := routePayloadLookup{}
	for _, candidate := range candidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		payload := &routePayload{}
		if err = yaml.Unmarshal(data, payload); err != nil {
			if !lookup.malformed {
				lookup.malformed = true
				lookup.malformedAt = candidate
				lookup.detail = strings.TrimSpace(err.Error())
			}
			continue
		}
		lookup.payload = payload
		lookup.found = true
		lookup.malformed = false
		lookup.malformedAt = ""
		lookup.detail = ""
		return lookup
	}
	return lookup
}

func readDQLPayload(dqlRoot string, layout compilePathLayout, namespace string, sourcePath string) routePayloadLookup {
	candidates := []string{}
	if strings.TrimSpace(sourcePath) != "" {
		candidates = append(candidates, sourcePath)
	}
	candidates = append(candidates, dqlSourceCandidates(dqlRoot, namespace)...)
	lookup := routePayloadLookup{}
	for _, candidate := range candidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		source := &shape.Source{
			Name: strings.TrimSuffix(filepath.Base(candidate), filepath.Ext(candidate)),
			Path: candidate,
			DQL:  string(data),
		}
		opts := []shape.CompileOption{}
		if marker := strings.TrimSpace(layout.dqlMarker); marker != "" {
			opts = append(opts, shape.WithDQLPathMarker(marker))
		}
		if rel := strings.TrimSpace(layout.routesRelative); rel != "" {
			opts = append(opts, shape.WithRoutesRelativePath(rel))
		}
		planned, err := New().Compile(context.Background(), source, opts...)
		if err != nil {
			if !lookup.malformed {
				lookup.malformed = true
				lookup.malformedAt = candidate
				lookup.detail = strings.TrimSpace(err.Error())
			}
			continue
		}
		result, ok := plan.ResultFrom(planned)
		if !ok || result == nil {
			if !lookup.malformed {
				lookup.malformed = true
				lookup.malformedAt = candidate
				lookup.detail = "unexpected compiled plan result"
			}
			continue
		}
		artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned)
		if err != nil {
			if !lookup.malformed {
				lookup.malformed = true
				lookup.malformedAt = candidate
				lookup.detail = strings.TrimSpace(err.Error())
			}
			continue
		}
		component, _ := shapeLoad.ComponentFrom(artifact)
		lookup.payload = routePayloadFromPlan(result, component)
		lookup.outputType = routeOutputReflectType(component, artifact.Resource)
		applyDQLRoutePayload(lookup.payload, source, result, namespace)
		lookup.found = true
		lookup.malformed = false
		lookup.malformedAt = ""
		lookup.detail = ""
		return lookup
	}
	return lookup
}

func (c *componentCollector) componentSourcePath(namespace string) string {
	index, err := c.lazyRouteIndex()
	if err != nil || index == nil {
		return ""
	}
	entries := index.ByNamespace[strings.ToLower(strings.TrimSpace(namespace))]
	if len(entries) != 1 || entries[0] == nil {
		return ""
	}
	return strings.TrimSpace(entries[0].SourcePath)
}

func (c *componentCollector) loadRoutePayload(namespace string, span dqlshape.Span) (*routePayload, bool) {
	key := strings.ToLower(strings.TrimSpace(namespace))
	if key == "" {
		return nil, false
	}
	lookup, ok := c.payloadCache[key]
	if !ok {
		lookup = readRoutePayload(c.routesRoot, namespace)
		if !lookup.found && strings.TrimSpace(c.dqlRoot) != "" {
			dqlLookup := readDQLPayload(c.dqlRoot, c.layout, namespace, c.componentSourcePath(namespace))
			if dqlLookup.found || dqlLookup.malformed {
				lookup = dqlLookup
			}
		}
		c.payloadCache[key] = lookup
	}
	if lookup.malformed && !lookup.found && !c.hasReported("invalid:"+key) {
		c.reportedDiag["invalid:"+key] = true
		message := "component route YAML malformed: " + namespace
		if strings.TrimSpace(lookup.malformedAt) != "" {
			message += " (" + lookup.malformedAt + ")"
		}
		hint := "fix route YAML format"
		if strings.TrimSpace(lookup.detail) != "" {
			hint += ": " + lookup.detail
		}
		c.diags = append(c.diags, &dqlshape.Diagnostic{
			Code:     dqldiag.CodeCompRouteInvalid,
			Severity: dqlshape.SeverityWarning,
			Message:  message,
			Hint:     hint,
			Span:     span,
		})
	}
	return lookup.payload, lookup.found
}

func (c *componentCollector) hasReported(key string) bool {
	if c == nil || c.reportedDiag == nil {
		return false
	}
	return c.reportedDiag[key]
}

func routeOutputType(payload *routePayload) string {
	if payload == nil {
		return ""
	}
	for _, route := range payload.Routes {
		if outputType := strings.TrimSpace(route.Handler.OutputType); outputType != "" {
			leaf := outputType
			if idx := strings.LastIndex(leaf, "."); idx >= 0 && idx+1 < len(leaf) {
				leaf = leaf[idx+1:]
			}
			leaf = strings.Trim(strings.TrimSpace(leaf), "*")
			if leaf != "" {
				return "*" + leaf
			}
		}
		if name := strings.TrimSpace(route.Output.Type.Name); name != "" {
			name = strings.Trim(name, "*")
			if name != "" {
				return "*" + name
			}
		}
	}
	for _, param := range payload.Resource.Parameters {
		if strings.EqualFold(strings.TrimSpace(param.In.Kind), string(state.KindOutput)) {
			if dataType := strings.TrimSpace(param.Schema.DataType); dataType != "" {
				return dataType
			}
			if name := strings.TrimSpace(param.Schema.Name); name != "" {
				name = strings.Trim(name, "*")
				if name != "" {
					return "*" + name
				}
			}
		}
	}
	for _, item := range payload.Resource.Types {
		if strings.EqualFold(strings.TrimSpace(item.Name), string(state.KindOutput)) {
			if dataType := strings.TrimSpace(item.DataType); dataType != "" {
				return dataType
			}
			return "*Output"
		}
	}
	return ""
}

func routeOutputPackage(payload *routePayload, outputType string) (string, string) {
	if payload == nil {
		return "", ""
	}
	if len(payload.Routes) > 0 {
		if pkg := strings.TrimSpace(payload.Routes[0].Output.Type.Package); pkg != "" {
			modulePath := routeTypeModulePath(payload, strings.TrimSpace(payload.Routes[0].Output.Type.Name))
			return pkg, modulePath
		}
	}
	leaf := strings.Trim(strings.TrimSpace(outputType), "*")
	if leaf == "" {
		return "", ""
	}
	for _, item := range payload.Resource.Types {
		name := strings.TrimSpace(item.Name)
		dataType := strings.Trim(strings.TrimSpace(item.DataType), "*")
		if strings.EqualFold(name, leaf) || strings.EqualFold(dataType, leaf) {
			return strings.TrimSpace(item.Package), strings.TrimSpace(item.ModulePath)
		}
	}
	for _, param := range payload.Resource.Parameters {
		if !strings.EqualFold(strings.TrimSpace(param.In.Kind), string(state.KindOutput)) {
			continue
		}
		if name := strings.Trim(strings.TrimSpace(param.Schema.Name), "*"); name == leaf {
			return strings.TrimSpace(param.Schema.Package), ""
		}
		if dataType := strings.Trim(strings.TrimSpace(param.Schema.DataType), "*"); dataType == leaf {
			return strings.TrimSpace(param.Schema.Package), ""
		}
	}
	return "", ""
}

func routeTypeModulePath(payload *routePayload, name string) string {
	name = strings.Trim(strings.TrimSpace(name), "*")
	if payload == nil || name == "" {
		return ""
	}
	for _, item := range payload.Resource.Types {
		if strings.EqualFold(strings.TrimSpace(item.Name), name) {
			return strings.TrimSpace(item.ModulePath)
		}
	}
	return ""
}

func componentRefSpan(raw, ref string) dqlshape.Span {
	offset := 0
	ref = strings.TrimSpace(ref)
	if ref != "" {
		if idx := strings.Index(raw, ref); idx >= 0 {
			offset = idx
		}
	}
	return relationSpan(raw, offset)
}

func routeYAMLCandidates(routesRoot, namespace string) []string {
	namespace = strings.Trim(namespace, "/")
	if namespace == "" {
		return nil
	}
	leaf := filepath.Base(namespace)
	return []string{
		filepath.Join(routesRoot, filepath.FromSlash(namespace)+".yaml"),
		filepath.Join(routesRoot, filepath.FromSlash(namespace), leaf+".yaml"),
	}
}

func routePayloadKey(payload *routePayload) string {
	if payload == nil {
		return ""
	}
	for _, route := range payload.Routes {
		if uri := strings.TrimSpace(route.URI); uri != "" {
			return normalizeRouteKey(strings.TrimSpace(route.Method), uri)
		}
	}
	return ""
}

func collectDQLSources(root string) ([]string, error) {
	var result []string
	err := filepath.WalkDir(root, func(candidate string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !isComponentDQLSourceFile(candidate) {
			return nil
		}
		result = append(result, candidate)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func isComponentDQLSourceFile(path string) bool {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(path)))
	return ext == ".dql" || ext == ".sql"
}

func dqlSourceCandidates(dqlRoot, namespace string) []string {
	namespace = strings.Trim(namespace, "/")
	if namespace == "" || strings.TrimSpace(dqlRoot) == "" {
		return nil
	}
	leaf := filepath.Base(namespace)
	base := filepath.Join(dqlRoot, filepath.FromSlash(namespace))
	return []string{
		base + ".dql",
		base + ".sql",
		filepath.Join(base, leaf+".dql"),
		filepath.Join(base, leaf+".sql"),
	}
}

func routePayloadFromPlan(result *plan.Result, component *shapeLoad.Component) *routePayload {
	if result == nil {
		return nil
	}
	payload := &routePayload{}
	for _, item := range result.Types {
		if item == nil {
			continue
		}
		payload.Resource.Types = append(payload.Resource.Types, struct {
			Name        string `yaml:"Name"`
			Alias       string `yaml:"Alias"`
			DataType    string `yaml:"DataType"`
			Cardinality string `yaml:"Cardinality"`
			Package     string `yaml:"Package"`
			ModulePath  string `yaml:"ModulePath"`
		}{
			Name:        strings.TrimSpace(item.Name),
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    strings.TrimSpace(item.DataType),
			Cardinality: strings.TrimSpace(item.Cardinality),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		})
	}
	ensureComponentOutputType(payload, component)
	for _, item := range result.States {
		if item == nil {
			continue
		}
		param := struct {
			Name string `yaml:"Name"`
			In   struct {
				Kind string `yaml:"Kind"`
				Name string `yaml:"Name"`
			} `yaml:"In"`
			Schema struct {
				DataType    string `yaml:"DataType"`
				Name        string `yaml:"Name"`
				Package     string `yaml:"Package"`
				Cardinality string `yaml:"Cardinality"`
			} `yaml:"Schema"`
		}{Name: strings.TrimSpace(item.Name)}
		if item.In != nil {
			param.In.Kind = string(item.In.Kind)
			param.In.Name = strings.TrimSpace(item.In.Name)
		}
		if item.Schema != nil {
			param.Schema.DataType = strings.TrimSpace(item.Schema.DataType)
			param.Schema.Name = strings.TrimSpace(item.Schema.Name)
			param.Schema.Package = strings.TrimSpace(item.Schema.Package)
			param.Schema.Cardinality = string(item.Schema.Cardinality)
		}
		payload.Resource.Parameters = append(payload.Resource.Parameters, param)
	}
	if outputType := componentOutputType(component, result); outputType != "" {
		payload.Routes = append(payload.Routes, struct {
			Method  string `yaml:"Method"`
			URI     string `yaml:"URI"`
			Handler struct {
				OutputType string `yaml:"OutputType"`
			} `yaml:"Handler"`
			Output struct {
				Cardinality string `yaml:"Cardinality"`
				Type        struct {
					Name    string `yaml:"Name"`
					Package string `yaml:"Package"`
				} `yaml:"Type"`
			} `yaml:"Output"`
		}{})
		payload.Routes[0].Handler.OutputType = outputType
		if name, pkg := componentOutputName(component); name != "" {
			payload.Routes[0].Output.Type.Name = name
			payload.Routes[0].Output.Type.Package = pkg
		}
	}
	return payload
}

func componentOutputType(component *shapeLoad.Component, result *plan.Result) string {
	if name, _ := componentOutputName(component); name != "" {
		return "*" + strings.Trim(name, "*")
	}
	if component != nil {
		for _, item := range component.Output {
			if item == nil {
				continue
			}
			if outputType := strings.TrimSpace(item.OutputDataType); outputType != "" {
				return outputType
			}
			if item.Schema != nil {
				if dataType := strings.TrimSpace(item.Schema.DataType); dataType != "" {
					return dataType
				}
				if name := strings.TrimSpace(item.Schema.Name); name != "" {
					return "*" + strings.Trim(name, "*")
				}
			}
		}
	}
	return planOutputType(result)
}

func routeOutputReflectType(component *shapeLoad.Component, resource *view.Resource) reflect.Type {
	if component == nil || resource == nil {
		return nil
	}
	pkgPath := ""
	if component.TypeContext != nil {
		pkgPath = strings.TrimSpace(component.TypeContext.PackagePath)
		if pkgPath == "" {
			pkgPath = strings.TrimSpace(component.TypeContext.DefaultPackage)
		}
	}
	params := resource.Parameters.FilterByKind(state.KindOutput)
	if len(params) == 0 {
		params = component.OutputParameters()
	}
	if len(params) == 0 {
		return nil
	}
	rt, err := params.ReflectType(pkgPath, resource.LookupType())
	if err != nil {
		return nil
	}
	return rt
}

func ensureComponentOutputType(payload *routePayload, component *shapeLoad.Component) {
	if payload == nil || component == nil {
		return
	}
	name, pkg := componentOutputName(component)
	modulePath := ""
	if component.TypeContext != nil {
		modulePath = strings.TrimSpace(component.TypeContext.PackagePath)
	}
	if name == "" || pkg == "" || modulePath == "" {
		return
	}
	for _, item := range payload.Resource.Types {
		if strings.EqualFold(strings.TrimSpace(item.Name), name) {
			return
		}
	}
	payload.Resource.Types = append(payload.Resource.Types, struct {
		Name        string `yaml:"Name"`
		Alias       string `yaml:"Alias"`
		DataType    string `yaml:"DataType"`
		Cardinality string `yaml:"Cardinality"`
		Package     string `yaml:"Package"`
		ModulePath  string `yaml:"ModulePath"`
	}{
		Name:       name,
		DataType:   "*" + name,
		Package:    pkg,
		ModulePath: modulePath,
	})
}

func componentOutputName(component *shapeLoad.Component) (string, string) {
	if component == nil {
		return "", ""
	}
	name := generatedComponentTypeBase(component) + "Output"
	if spec := component.TypeSpecs["output"]; spec != nil && strings.TrimSpace(spec.TypeName) != "" {
		name = strings.TrimSpace(spec.TypeName)
	}
	pkg := ""
	if component.TypeContext != nil {
		pkg = strings.TrimSpace(component.TypeContext.PackagePath)
		if pkg == "" {
			pkg = strings.TrimSpace(component.TypeContext.DefaultPackage)
		}
	}
	return name, pkg
}

func generatedComponentTypeBase(component *shapeLoad.Component) string {
	if component == nil {
		return "Component"
	}
	name := strings.TrimSpace(component.RootView)
	if name == "" {
		name = strings.TrimSpace(component.Name)
	}
	if name == "" {
		name = "Component"
	}
	return state.SanitizeTypeName(name)
}

func planOutputType(result *plan.Result) string {
	if result == nil {
		return ""
	}
	for _, item := range result.States {
		if item == nil || !strings.EqualFold(item.KindString(), string(state.KindOutput)) {
			continue
		}
		if outputType := strings.TrimSpace(item.OutputDataType); outputType != "" {
			return outputType
		}
		if item.Schema != nil {
			if dataType := strings.TrimSpace(item.Schema.DataType); dataType != "" {
				return dataType
			}
			if name := strings.TrimSpace(item.Schema.Name); name != "" {
				return "*" + strings.Trim(name, "*")
			}
		}
	}
	for _, item := range result.Types {
		if item == nil || !strings.EqualFold(strings.TrimSpace(item.Name), "Output") {
			continue
		}
		if dataType := strings.TrimSpace(item.DataType); dataType != "" {
			return dataType
		}
		return "*Output"
	}
	return ""
}

func applyDQLRoutePayload(payload *routePayload, source *shape.Source, result *plan.Result, namespace string) {
	if payload == nil || len(payload.Routes) == 0 {
		return
	}
	settings := extractRuleSettings(source, nil)
	if result != nil {
		settings = extractRuleSettings(source, result.Directives)
	}
	method := httpMethod(settings)
	uri := strings.TrimSpace(settings.URI)
	if uri == "" {
		uri = inferDefaultURI(namespace)
	}
	payload.Routes[0].Method = method
	payload.Routes[0].URI = normalizeURI(uri)
}

func httpMethod(settings *ruleSettings) string {
	if settings == nil {
		return "GET"
	}
	methods := parseRouteMethods(settings.Method)
	if len(methods) == 0 {
		return "GET"
	}
	return strings.ToUpper(strings.TrimSpace(methods[0]))
}
