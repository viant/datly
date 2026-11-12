package plan

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ValidateRelations validates relation links in generated route YAML.
func ValidateRelations(routeYAML []byte) error {
	var root map[string]any
	if err := yaml.Unmarshal(routeYAML, &root); err != nil {
		return err
	}
	views := extractViews(root)
	if len(views) == 0 {
		return nil
	}
	lineIndex, err := collectViewMeta(routeYAML)
	if err != nil {
		return err
	}
	viewIndex := buildViewIndex(views, lineIndex)
	issues := collectRelationIssues(viewIndex)
	if len(issues) == 0 {
		return nil
	}
	return fmt.Errorf("dql plan relation validation failed:\n- %s", strings.Join(issues, "\n- "))
}

func buildViewIndex(views []any, lineIndex map[string]*viewMeta) map[string]*viewMeta {
	viewIndex := map[string]*viewMeta{}
	for _, item := range views {
		viewMap := toFlatMap(item)
		if viewMap == nil {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(viewMap["Name"]))
		if name == "" {
			continue
		}
		meta := lineIndex[name]
		if meta == nil {
			meta = &viewMeta{Name: name, Namespaces: map[string]bool{}}
			lineIndex[name] = meta
		}
		applyViewRuntimeSQLMeta(viewMap, meta)
		viewIndex[name] = meta
	}
	return viewIndex
}

func applyViewRuntimeSQLMeta(viewMap map[string]any, meta *viewMeta) {
	template := toFlatMap(viewMap["Template"])
	if template != nil {
		source := strings.TrimSpace(fmt.Sprint(template["Source"]))
		aliases, projection, hasSQL := analyzeSQL(source)
		if hasSQL {
			meta.HasSQL = true
		}
		if len(aliases) > 0 {
			meta.Aliases = aliases
		}
		meta.Projection = projection
	}
	selector := toFlatMap(viewMap["Selector"])
	if selector != nil {
		registerAlias(meta.Namespaces, fmt.Sprint(selector["Namespace"]))
	}
	if meta.Aliases == nil {
		meta.Aliases = map[string]bool{}
	}
	if meta.Namespaces == nil {
		meta.Namespaces = map[string]bool{}
	}
	for alias := range meta.Aliases {
		meta.Namespaces[alias] = true
	}
	if meta.Projection.Columns == nil {
		meta.Projection.Columns = map[string]bool{}
	}
}

func collectRelationIssues(viewIndex map[string]*viewMeta) []string {
	var issues []string
	for _, parent := range viewIndex {
		for _, rel := range parent.Relations {
			ref := viewIndex[strings.TrimSpace(rel.Ref)]
			for i := 0; i < rel.PairCount; i++ {
				left, right := linkAt(rel.On, i), linkAt(rel.OfOn, i)
				issues = append(issues, validateParentLink(parent, rel, i, left)...)
				issues = append(issues, validateRefLink(parent, ref, rel, i, right)...)
			}
		}
	}
	return issues
}

func validateParentLink(parent *viewMeta, rel relationMeta, i int, left *relationLink) []string {
	if left == nil {
		return []string{fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=parent: missing On link entry", rel.Line, parent.Name, rel.Name, rel.Holder, i)}
	}
	return validateLink(parent, rel, "parent", i, *left)
}

func validateRefLink(parent, ref *viewMeta, rel relationMeta, i int, right *relationLink) []string {
	if right == nil {
		return []string{fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=ref: missing Of.On link entry", rel.Line, parent.Name, rel.Name, rel.Holder, i)}
	}
	if ref != nil {
		return validateLink(ref, rel, "ref", i, *right)
	}
	if strings.TrimSpace(rel.Ref) == "" {
		return nil
	}
	return []string{fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=ref: referenced view %q not found", right.Line, parent.Name, rel.Name, rel.Holder, i, rel.Ref)}
}

func linkAt(links []relationLink, i int) *relationLink {
	if i < 0 || i >= len(links) {
		return nil
	}
	return &links[i]
}

func validateLink(view *viewMeta, rel relationMeta, side string, index int, link relationLink) []string {
	if view == nil {
		return nil
	}
	line := link.Line
	if line == 0 {
		line = rel.Line
	}
	column := strings.TrimSpace(link.Column)
	alias := strings.TrimSpace(link.Namespace)
	if column == "" {
		return []string{fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=%s: empty column", line, view.Name, rel.Name, rel.Holder, index, side)}
	}

	var issues []string
	columnProjected := true
	if view.HasSQL && !view.Projection.HasStar && len(view.Projection.Columns) > 0 {
		columnProjected = hasProjectionColumn(view.Projection.Columns, column)
		if !columnProjected {
			issues = append(issues, fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=%s alias=%q column=%q: column not projected (columns=%v)", line, view.Name, rel.Name, rel.Holder, index, side, alias, column, sortedKeys(view.Projection.Columns)))
		}
	}
	if alias != "" && view.HasSQL && !columnProjected && !view.Namespaces[strings.ToLower(alias)] {
		issues = append(issues, fmt.Sprintf("line %d view=%q relation=%q holder=%q link=%d side=%s alias=%q column=%q: alias not present in SQL/selector namespace (namespaces=%v)", line, view.Name, rel.Name, rel.Holder, index, side, alias, column, sortedKeys(view.Namespaces)))
	}
	return issues
}

func hasProjectionColumn(columns map[string]bool, column string) bool {
	for _, candidate := range projectionCandidates(column) {
		if columns[candidate] {
			return true
		}
	}
	return false
}

func projectionCandidates(column string) []string {
	column = strings.TrimSpace(column)
	if column == "" {
		return nil
	}
	result := []string{normalizedProjectionKey(column)}
	if i := strings.LastIndex(column, "."); i != -1 && i+1 < len(column) {
		result = append(result, normalizedProjectionKey(column[i+1:]))
	}
	return result
}

func normalizedProjectionKey(value string) string {
	return strings.ToLower(strings.Trim(value, "`\"' "))
}

func extractViews(root map[string]any) []any {
	resource := toFlatMap(root["Resource"])
	if resource == nil {
		return nil
	}
	return canonicalSlice(resource["Views"])
}

func collectViewMeta(routeYAML []byte) (map[string]*viewMeta, error) {
	var rootNode yaml.Node
	if err := yaml.Unmarshal(routeYAML, &rootNode); err != nil {
		return nil, err
	}
	return parseViewMetaNodes(&rootNode), nil
}

func sortedKeys(index map[string]bool) []string {
	ret := make([]string, 0, len(index))
	for key := range index {
		ret = append(ret, key)
	}
	sort.Strings(ret)
	return ret
}
