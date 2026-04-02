package plan

import (
	"fmt"
	"github.com/viant/datly/repository/shape/dql/ir"
	"gopkg.in/yaml.v3"
	"regexp"
	"sort"
	"strings"
)

var (
	routeFields       = []string{"Name", "URI", "Method", "Description", "MCPTool", "Service"}
	routeInputFields  = []string{"Type", "Parameters"}
	routeOutputFields = []string{"Type", "Parameters", "Exclude", "CaseFormat", "Tag"}
	parameterFields   = []string{"Name", "Required", "Tag", "ErrorStatusCode", "Cacheable", "Scope", "Connector", "Value", "Limit"}
	viewFields        = []string{"Name", "Table", "Mode", "AllowNulls", "RelationalConcurrency"}
	selectorFields    = []string{"Constraints", "Limit", "Namespace"}
	templateFields    = []string{"SourceURL", "Source", "Summary"}
)

var tagMatcher = regexp.MustCompile(`([A-Za-z0-9_]+):"([^"]*)"`)
var veltyPlaceholderBraced = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Result is canonicalized route YAML representation.
type Result struct {
	Canonical map[string]any
}

// Build creates a canonical map from route YAML.
func Build(routeYAML []byte) (*Result, error) {
	if len(routeYAML) == 0 {
		return nil, fmt.Errorf("dql plan: empty YAML")
	}
	var root map[string]any
	if err := yaml.Unmarshal(routeYAML, &root); err != nil {
		return nil, err
	}
	canonical := projectCanonical(root)
	return &Result{Canonical: canonical}, nil
}

// BuildFromIR canonicalizes IR without requiring YAML rendering/parsing.
func BuildFromIR(doc *ir.Document) (*Result, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("dql plan: empty IR")
	}
	canonical := projectCanonical(doc.Root)
	return &Result{Canonical: canonical}, nil
}

func projectCanonical(root map[string]any) map[string]any {
	out := map[string]any{}
	rootRefs := collectRootViewRefs(root["Routes"])
	if routes, ok := root["Routes"]; ok {
		if canonical := canonicalRoutes(routes); len(canonical) > 0 {
			out["Routes"] = canonical
		}
	}
	if resource := toFlatMap(root["Resource"]); resource != nil {
		if views := canonicalViews(resource["Views"], rootRefs); len(views) > 0 {
			out["Resource"] = map[string]any{"Views": views}
		}
	}
	return out
}

func canonicalRoutes(raw any) []any {
	items := canonicalSlice(raw)
	var routes []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			routes = append(routes, canonicalRoute(normalized))
		}
	}
	sort.SliceStable(routes, func(i, j int) bool {
		return mapStringCompare(routes[i], routes[j], "Name", "URI")
	})
	result := make([]any, len(routes))
	for i, r := range routes {
		result[i] = r
	}
	return result
}

func canonicalRoute(src map[string]any) map[string]any {
	out := map[string]any{}
	copyFields(out, src, routeFields)
	if view := canonicalRouteView(src["View"]); view != nil {
		out["View"] = view
	}
	if input := canonicalRouteIO(src["Input"], routeInputFields, true); len(input) > 0 {
		out["Input"] = input
	}
	if output := canonicalRouteIO(src["Output"], routeOutputFields, false); len(output) > 0 {
		out["Output"] = output
	}
	if with := canonicalStringList(src["With"]); len(with) > 0 {
		out["With"] = with
	}
	return out
}

func canonicalRouteView(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		return filterMap(normalized, []string{"Ref"})
	}
	return nil
}

func canonicalRouteIO(raw any, allowed []string, includeTypeName bool) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := map[string]any{}
		typeMap := toFlatMap(normalized["Type"])
		for _, key := range allowed {
			val, ok := normalized[key]
			if !ok {
				if key != "Parameters" {
					continue
				}
			}
			switch key {
			case "Type":
				if canonical := canonicalTypeWithName(typeMap, includeTypeName); len(canonical) > 0 {
					out["Type"] = canonical
				}
			case "Parameters":
				parameterRaw := val
				if typeMap != nil && typeMap["Parameters"] != nil {
					parameterRaw = typeMap["Parameters"]
				}
				if canonical := canonicalParameters(parameterRaw); len(canonical) > 0 {
					out["Parameters"] = canonical
				}
			default:
				out[key] = normalizeValue(val)
			}
		}
		return out
	}
	return nil
}

func canonicalTypeWithName(raw any, includeName bool) map[string]any {
	keys := []string{"Package"}
	if includeName {
		keys = []string{"Name", "Package"}
	}
	return filterMap(toFlatMap(raw), keys)
}

func canonicalParameters(raw any) []any {
	items := canonicalSlice(raw)
	var params []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			if param := canonicalParameter(normalized); len(param) > 0 {
				params = append(params, param)
			}
		}
	}
	sort.SliceStable(params, func(i, j int) bool {
		return mapStringCompare(params[i], params[j], "Name")
	})
	result := make([]any, len(params))
	for i, p := range params {
		result[i] = p
	}
	return result
}

func canonicalParameter(src map[string]any) map[string]any {
	if in := canonicalIn(src["In"]); len(in) > 0 && fmt.Sprint(in["Kind"]) == "component" {
		return nil
	} else if isSyntheticSubstituteParameter(src, in) {
		return nil
	}
	out := map[string]any{}
	copyFields(out, src, parameterFields)
	if tag := canonicalTag(src["Tag"]); len(tag) > 0 {
		out["TagMeta"] = tag
	}
	if in := canonicalIn(src["In"]); len(in) > 0 {
		out["In"] = in
	}
	if schema := canonicalSchema(src["Schema"]); len(schema) > 0 {
		out["Schema"] = schema
	}
	if output := canonicalOutput(src["Output"]); len(output) > 0 {
		out["Output"] = output
	}
	if preds := canonicalPredicates(src["Predicates"]); len(preds) > 0 {
		out["Predicates"] = preds
	}
	if loc := canonicalLocationInput(src["LocationInput"]); len(loc) > 0 {
		out["LocationInput"] = loc
	}
	return out
}

func isSyntheticSubstituteParameter(src map[string]any, in map[string]any) bool {
	if strings.ToLower(fmt.Sprint(in["Kind"])) != "form" {
		return false
	}
	name := strings.ToLower(fmt.Sprint(src["Name"]))
	return strings.HasSuffix(name, "_table_suffix")
}

func canonicalIn(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"Kind", "Name"})
}

func canonicalSchema(raw any) map[string]any {
	out := filterMap(toFlatMap(raw), []string{"Name", "Package", "DataType", "Cardinality"})
	if pkg, ok := out["Package"].(string); ok {
		out["Package"] = normalizeSchemaPackage(pkg)
	}
	return out
}

func canonicalPredicates(raw any) []any {
	items := canonicalSlice(raw)
	var preds []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			entry := filterMap(normalized, []string{"Name", "Ensure", "Group"})
			if args, ok := normalized["Args"]; ok {
				entry["Args"] = normalizeValue(args)
			}
			if len(entry) > 0 {
				preds = append(preds, entry)
			}
		}
	}
	sort.SliceStable(preds, func(i, j int) bool {
		return mapStringCompare(preds[i], preds[j], "Name")
	})
	result := make([]any, len(preds))
	for i, p := range preds {
		result[i] = p
	}
	return result
}

func canonicalLocationInput(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := map[string]any{}
		copyFields(out, normalized, []string{"Name", "Package"})
		if params := canonicalParameters(normalized["Parameters"]); len(params) > 0 {
			out["Parameters"] = params
		}
		return out
	}
	return nil
}

func canonicalOutput(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := filterMap(normalized, []string{"Name", "Args"})
		if schema := canonicalSchema(normalized["Schema"]); len(schema) > 0 {
			out["Schema"] = schema
		}
		return out
	}
	return nil
}

func canonicalTag(raw any) map[string]any {
	text := fmt.Sprint(raw)
	if text == "" || text == "<nil>" {
		return nil
	}
	parsed := map[string]string{}
	for _, group := range tagMatcher.FindAllStringSubmatch(text, -1) {
		if len(group) < 3 {
			continue
		}
		parsed[group[1]] = group[2]
	}
	if len(parsed) == 0 {
		return map[string]any{"Raw": text}
	}
	return map[string]any{
		"Raw":   text,
		"Pairs": parsed,
	}
}

func canonicalViews(raw any, roots []string) []any {
	items := canonicalSlice(raw)
	allowed := collectReachableViews(items, roots)
	var views []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			name := fmt.Sprint(normalized["Name"])
			if len(allowed) > 0 && !allowed[name] {
				continue
			}
			view := canonicalView(normalized)
			if len(view) > 0 {
				views = append(views, view)
			}
		}
	}
	sort.SliceStable(views, func(i, j int) bool {
		return mapStringCompare(views[i], views[j], "Name")
	})
	result := make([]any, len(views))
	for i, v := range views {
		result[i] = v
	}
	return result
}

func canonicalView(src map[string]any) map[string]any {
	out := map[string]any{}
	copyFields(out, src, viewFields)
	if partitioned := canonicalPartitioned(src["Partitioned"]); len(partitioned) > 0 {
		out["Partitioned"] = partitioned
	}
	if connector := canonicalConnector(src["Connector"]); len(connector) > 0 {
		out["Connector"] = connector
	}
	if selector := canonicalSelector(src["Selector"]); len(selector) > 0 {
		out["Selector"] = selector
	}
	if strings.ToLower(fmt.Sprint(src["Mode"])) != "sqlexec" {
		if template := canonicalTemplate(src["Template"]); len(template) > 0 {
			out["Template"] = template
		}
	}
	return out
}

func canonicalPartitioned(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"DataType", "Concurrency"})
}

func canonicalConnector(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"Ref"})
}

func canonicalSelector(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := map[string]any{}
		copyFields(out, normalized, selectorFields)
		if constraints := canonicalSelectorConstraints(normalized["Constraints"]); len(constraints) > 0 {
			out["Constraints"] = constraints
		}
		return out
	}
	return nil
}

func canonicalSelectorConstraints(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"Criteria", "Filterable", "Limit", "Offset", "OrderBy", "Projection"})
}

func canonicalTemplate(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := map[string]any{}
		copyFields(out, normalized, templateFields)
		if summary := canonicalSummary(normalized["Summary"]); len(summary) > 0 {
			out["Summary"] = summary
		}
		if with := canonicalTemplateWith(normalized["With"]); len(with) > 0 {
			out["With"] = with
		}
		return out
	}
	return nil
}

func canonicalSummary(raw any) map[string]any {
	if normalized := toFlatMap(raw); normalized != nil {
		out := copyMap(filterMap(normalized, []string{"Kind", "Name", "Source"}))
		if schema := canonicalSummarySchema(normalized["Schema"]); len(schema) > 0 {
			out["Schema"] = schema
		}
		return out
	}
	return nil
}

func canonicalSummarySchema(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"Name", "Package", "DataType"})
}

func canonicalTemplateWith(raw any) []any {
	return canonicalWithList(raw)
}

func canonicalViewWith(raw any) []any {
	return canonicalWithList(raw)
}

func canonicalWithList(raw any) []any {
	items := canonicalSlice(raw)
	var nodes []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			if node := canonicalWithNode(normalized); len(node) > 0 {
				nodes = append(nodes, node)
			}
		}
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		return mapStringCompare(nodes[i], nodes[j], "Name", "Holder")
	})
	result := make([]any, len(nodes))
	for i, n := range nodes {
		result[i] = n
	}
	return result
}

func canonicalWithNode(src map[string]any) map[string]any {
	out := map[string]any{}
	copyFields(out, src, []string{"Name", "Holder", "Cardinality", "IncludeColumn"})
	if of := canonicalOf(src["Of"]); len(of) > 0 {
		out["Of"] = of
	}
	if on := canonicalOn(src["On"]); len(on) > 0 {
		out["On"] = on
	}
	return out
}

func canonicalOf(raw any) map[string]any {
	return filterMap(toFlatMap(raw), []string{"Name", "Ref"})
}

func canonicalOn(raw any) []any {
	items := canonicalSlice(raw)
	var list []map[string]any
	for _, item := range items {
		if normalized := toFlatMap(item); normalized != nil {
			entry := filterMap(normalized, []string{"Column", "Field"})
			if len(entry) > 0 {
				list = append(list, entry)
			}
		}
	}
	sort.SliceStable(list, func(i, j int) bool {
		return mapStringCompare(list[i], list[j], "Column", "Field")
	})
	result := make([]any, len(list))
	for i, n := range list {
		result[i] = n
	}
	return result
}

func canonicalSlice(raw any) []any {
	if normalized, ok := normalizeValue(raw).([]any); ok {
		return normalized
	}
	return nil
}

func toFlatMap(raw any) map[string]any {
	if normalized, ok := normalizeValue(raw).(map[string]any); ok {
		return normalized
	}
	return nil
}

func normalizeValue(v any) any {
	switch actual := v.(type) {
	case map[string]any:
		ret := map[string]any{}
		for k, val := range actual {
			ret[k] = normalizeValue(val)
		}
		return ret
	case map[any]any:
		ret := map[string]any{}
		for k, val := range actual {
			ret[fmt.Sprint(k)] = normalizeValue(val)
		}
		return ret
	case []any:
		ret := make([]any, len(actual))
		for i, item := range actual {
			ret[i] = normalizeValue(item)
		}
		return ret
	default:
		if text, ok := actual.(string); ok {
			return normalizeTextValue(text)
		}
		return actual
	}
}

func normalizeTextValue(text string) string {
	if text == "" {
		return text
	}
	return veltyPlaceholderBraced.ReplaceAllString(text, `$$$1`)
}

func normalizeSchemaPackage(pkg string) string {
	if pkg == "auto" {
		return "automation"
	}
	if pkg == "allocator" {
		return "bidalloc"
	}
	return pkg
}

func copyFields(dst, src map[string]any, keys []string) {
	for _, key := range keys {
		if val, ok := src[key]; ok {
			dst[key] = normalizeValue(val)
		}
	}
}

func filterMap(src map[string]any, keys []string) map[string]any {
	if src == nil {
		return nil
	}
	out := map[string]any{}
	for _, key := range keys {
		if val, ok := src[key]; ok {
			dst := normalizeValue(val)
			if dst != nil {
				out[key] = dst
			}
		}
	}
	return out
}

func canonicalStringList(raw any) []string {
	items := canonicalSlice(raw)
	var list []string
	for _, item := range items {
		switch val := item.(type) {
		case string:
			list = append(list, val)
		default:
			list = append(list, fmt.Sprint(val))
		}
	}
	sort.Strings(list)
	return list
}

func mapStringCompare(a, b map[string]any, keys ...string) bool {
	for _, key := range keys {
		ai := fmt.Sprint(a[key])
		bi := fmt.Sprint(b[key])
		if ai != bi {
			return ai < bi
		}
	}
	return fmt.Sprint(a) < fmt.Sprint(b)
}

func copyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func collectRootViewRefs(raw any) []string {
	items := canonicalSlice(raw)
	unique := map[string]bool{}
	var result []string
	for _, item := range items {
		route := toFlatMap(item)
		if route == nil {
			continue
		}
		view := toFlatMap(route["View"])
		if view == nil {
			continue
		}
		ref := strings.TrimSpace(fmt.Sprint(view["Ref"]))
		if ref == "" || unique[ref] {
			continue
		}
		unique[ref] = true
		result = append(result, ref)
	}
	sort.Strings(result)
	return result
}

func collectReachableViews(rawViews []any, roots []string) map[string]bool {
	if len(roots) == 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, root := range roots {
		if root != "" {
			seen[root] = true
		}
	}
	return seen
}
