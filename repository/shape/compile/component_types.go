package compile

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
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
		visited:       map[string]componentVisitState{},
		outputByRoute: map[string]string{},
		typesByName:   map[string]*plan.Type{},
		payloadCache:  map[string]routePayloadLookup{},
		reportedDiag:  map[string]bool{},
	}
	if strings.TrimSpace(sourceNamespace) != "" {
		collector.collect(sourceNamespace, relationSpan(source.DQL, 0), false)
	}

	for _, stateItem := range result.States {
		if stateItem == nil || !strings.EqualFold(strings.TrimSpace(stateItem.Kind), "component") {
			continue
		}
		ref := strings.TrimSpace(stateItem.In)
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
		outputType, ok := collector.collect(namespace, componentRefSpan(source.DQL, ref), true)
		if ok && strings.TrimSpace(stateItem.DataType) == "" {
			stateItem.DataType = strings.TrimSpace(outputType)
		}
	}

	names := make([]string, 0, len(collector.typesByName))
	for name := range collector.typesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	existing := map[string]bool{}
	reportedCollision := map[string]bool{}
	for _, item := range result.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		existing[strings.ToLower(strings.TrimSpace(item.Name))] = true
	}
	for _, name := range names {
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
	visited       map[string]componentVisitState
	outputByRoute map[string]string
	typesByName   map[string]*plan.Type
	payloadCache  map[string]routePayloadLookup
	reportedDiag  map[string]bool
	diags         []*dqlshape.Diagnostic
}

type routePayloadLookup struct {
	payload     *routePayload
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

	for _, param := range payload.Resource.Parameters {
		if !strings.EqualFold(strings.TrimSpace(param.In.Kind), "component") {
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

func sourceRoots(sourcePath string) (platformRoot, routesRoot, dqlRoot string, ok bool) {
	return sourceRootsWithLayout(sourcePath, defaultCompilePathLayout())
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

func loadRoutePayload(routesRoot, namespace string) (*routePayload, bool) {
	lookup := readRoutePayload(routesRoot, namespace)
	return lookup.payload, lookup.found
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

func (c *componentCollector) loadRoutePayload(namespace string, span dqlshape.Span) (*routePayload, bool) {
	key := strings.ToLower(strings.TrimSpace(namespace))
	if key == "" {
		return nil, false
	}
	lookup, ok := c.payloadCache[key]
	if !ok {
		lookup = readRoutePayload(c.routesRoot, namespace)
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
		if strings.EqualFold(strings.TrimSpace(param.In.Kind), "output") {
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
		if strings.EqualFold(strings.TrimSpace(item.Name), "output") {
			if dataType := strings.TrimSpace(item.DataType); dataType != "" {
				return dataType
			}
			return "*Output"
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
