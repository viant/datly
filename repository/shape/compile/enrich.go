package compile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	"github.com/viant/datly/repository/shape/plan"
	"gopkg.in/yaml.v3"
)

var (
	ruleHeaderExpr  = regexp.MustCompile(`(?s)^\s*/\*\s*(\{.*?\})\s*\*/`)
	embedExpr       = regexp.MustCompile(`(?is)\$\{\s*embed:\s*([^}]+)\}`)
	fromTableExpr   = regexp.MustCompile(`(?is)\bfrom\s+([a-zA-Z_$][a-zA-Z0-9_$.{}/]*)`)
	summaryJoinExpr = regexp.MustCompile(`(?is)\bjoin\s*\((.*?)\)\s*summary\s+on\s+1\s*=\s*1`)
	joinEmbedExpr   = regexp.MustCompile(`(?is)\bjoin\s*\(\s*\$\{\s*embed:\s*([^}]+)\}\s*\)\s*(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*)`)
	joinBodyExpr    = regexp.MustCompile(`(?is)\bjoin\s*\((.*?)\)\s*(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s+on\b`)
)

type ruleSettings struct {
	Connector string `json:"Connector"`
	Name      string `json:"Name"`
	Type      string `json:"Type"`
	Method    string `json:"Method"`
	URI       string `json:"URI"`
}

func applySourceParityEnrichment(result *plan.Result, source *shape.Source) {
	applySourceParityEnrichmentWithLayout(result, source, defaultCompilePathLayout())
}

func applySourceParityEnrichmentWithLayout(result *plan.Result, source *shape.Source, layout compilePathLayout) {
	if result == nil || len(result.Views) == 0 {
		return
	}
	settings := extractRuleSettings(source)
	legacyViews := loadLegacyRouteViewAttrsWithLayout(source, settings, layout)
	baseDir := sourceSQLBaseDir(source)
	module := sourceModuleWithLayout(source, layout)
	sourceName := pipeline.SanitizeName(source.Name)
	joinEmbedRefs := map[string]string{}
	joinSubqueryBodies := map[string]string{}
	if len(result.Views) > 0 && result.Views[0] != nil {
		sqlForJoinExtract := result.Views[0].SQL
		if source != nil && strings.TrimSpace(source.DQL) != "" {
			sqlForJoinExtract = source.DQL
		}
		joinEmbedRefs = extractJoinEmbedRefs(sqlForJoinExtract)
		joinSubqueryBodies = extractJoinSubqueryBodies(sqlForJoinExtract)
	}
	for idx, item := range result.Views {
		if item == nil {
			continue
		}
		if legacy, ok := lookupLegacyRouteViewAttr(legacyViews, item.Name); ok {
			if legacy.Mode != "" {
				item.Mode = legacy.Mode
			}
			if legacy.Module != "" {
				item.Module = legacy.Module
			}
			if legacy.AllowNulls != nil {
				value := *legacy.AllowNulls
				item.AllowNulls = &value
			}
			if legacy.SelectorNamespace != "" {
				item.SelectorNamespace = legacy.SelectorNamespace
			}
			if legacy.SelectorNoLimit != nil {
				value := *legacy.SelectorNoLimit
				item.SelectorNoLimit = &value
			}
			if legacy.SchemaType != "" {
				item.SchemaType = legacy.SchemaType
			}
			if legacy.Cardinality != "" {
				item.Cardinality = legacy.Cardinality
			}
			if legacy.HasSummary != nil && *legacy.HasSummary && strings.TrimSpace(item.Summary) == "" {
				item.Summary = "legacy-summary"
			}
		}
		if item.SQLURI == "" && baseDir != "" {
			item.SQLURI = baseDir + "/" + item.Name + ".sql"
		}
		if item.Module == "" {
			item.Module = module
		}
		if item.SelectorNamespace == "" {
			item.SelectorNamespace = defaultSelectorNamespace(item.Name)
		}
		if item.SchemaType == "" {
			item.SchemaType = defaultSchemaType(item.Name, settings, idx == 0)
		}
		if shouldInferTable(item) {
			candidateSQL := item.SQL
			if strings.TrimSpace(candidateSQL) == "" {
				candidateSQL = item.Table
			}
			if table := inferTableFromSQL(candidateSQL, source); table != "" {
				item.Table = table
			}
		}
		if strings.HasPrefix(strings.TrimSpace(item.Table), "(") || normalizedTemplatePlaceholderTable(strings.TrimSpace(item.Table)) {
			if ref, ok := joinEmbedRefs[item.Name]; ok {
				if table := inferTableFromEmbedRef(source, ref); table != "" {
					item.Table = table
				}
			}
			if body, ok := joinSubqueryBodies[item.Name]; ok {
				if table := inferTableFromSQL(body, source); table != "" {
					item.Table = table
				}
			}
			if table := inferTableFromSiblingSQL(item.Name, source); table != "" {
				item.Table = table
			}
		}
		if item.Connector == "" && settings.Connector != "" {
			item.Connector = settings.Connector
		}
		if item.Connector == "" && source != nil && strings.TrimSpace(source.Connector) != "" {
			item.Connector = strings.TrimSpace(source.Connector)
		}
		if item.Connector == "" {
			item.Connector = inferConnector(item, source)
		}
		if item.Summary == "" {
			item.Summary = extractSummarySQL(item.SQL)
			if item.Summary == "" && source != nil {
				item.Summary = extractSummarySQL(source.DQL)
			}
		}
	}
	if source != nil && strings.TrimSpace(source.Path) != "" {
		normalizeRootViewName(result, sourceName, settings)
	}
}

type legacyRouteViewAttr struct {
	Name              string
	Mode              string
	Module            string
	AllowNulls        *bool
	SelectorNamespace string
	SelectorNoLimit   *bool
	SchemaType        string
	Cardinality       string
	HasSummary        *bool
}

func loadLegacyRouteViewAttrs(source *shape.Source, settings *ruleSettings) []legacyRouteViewAttr {
	return loadLegacyRouteViewAttrsWithLayout(source, settings, defaultCompilePathLayout())
}

func loadLegacyRouteViewAttrsWithLayout(source *shape.Source, settings *ruleSettings, layout compilePathLayout) []legacyRouteViewAttr {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return nil
	}
	platformRoot, relativeDir, stem, ok := platformPathParts(source.Path, layout)
	if !ok {
		return nil
	}
	typeExpr := ""
	if settings != nil {
		typeExpr = strings.TrimSpace(settings.Type)
	}
	typeExpr = strings.Trim(typeExpr, `"'`)
	typeExpr = strings.TrimSuffix(typeExpr, ".Handler")
	typeStem := ""
	if typeExpr != "" {
		typeStem = filepath.Base(filepath.FromSlash(typeExpr))
	}
	routesRoot := joinRelativePath(platformRoot, layout.routesRelative)
	routesBase := filepath.Join(routesRoot, filepath.FromSlash(relativeDir))
	candidates := legacyRouteYAMLCandidates(routesBase, stem, typeStem)
	for _, candidate := range candidates {
		if attrs := parseLegacyRouteViewAttrs(candidate); len(attrs) > 0 {
			return attrs
		}
	}
	return nil
}

func parseLegacyRouteViewAttrs(path string) []legacyRouteViewAttr {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var payload struct {
		Resource struct {
			Views []struct {
				Name       string `yaml:"Name"`
				Mode       string `yaml:"Mode"`
				Module     string `yaml:"Module"`
				AllowNulls *bool  `yaml:"AllowNulls"`
				Selector   struct {
					Namespace string `yaml:"Namespace"`
					NoLimit   *bool  `yaml:"NoLimit"`
				} `yaml:"Selector"`
				Template struct {
					Summary *struct{} `yaml:"Summary"`
				} `yaml:"Template"`
				Schema struct {
					Cardinality string `yaml:"Cardinality"`
					DataType    string `yaml:"DataType"`
					Name        string `yaml:"Name"`
				} `yaml:"Schema"`
			} `yaml:"Views"`
		} `yaml:"Resource"`
	}
	if err = yaml.Unmarshal(data, &payload); err != nil {
		return nil
	}
	result := make([]legacyRouteViewAttr, 0, len(payload.Resource.Views))
	for _, item := range payload.Resource.Views {
		cardinality := strings.TrimSpace(item.Schema.Cardinality)
		if cardinality != "" {
			cardinality = strings.ToLower(cardinality)
		}
		result = append(result, legacyRouteViewAttr{
			Name:              strings.TrimSpace(item.Name),
			Mode:              strings.TrimSpace(item.Mode),
			Module:            strings.TrimSpace(item.Module),
			AllowNulls:        item.AllowNulls,
			SelectorNamespace: strings.TrimSpace(item.Selector.Namespace),
			SelectorNoLimit:   item.Selector.NoLimit,
			SchemaType:        firstNonEmptyString(strings.TrimSpace(item.Schema.DataType), strings.TrimSpace(item.Schema.Name)),
			Cardinality:       cardinality,
			HasSummary: func() *bool {
				if item.Template.Summary == nil {
					return nil
				}
				value := true
				return &value
			}(),
		})
	}
	return result
}

func lookupLegacyRouteViewAttr(items []legacyRouteViewAttr, name string) (legacyRouteViewAttr, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return legacyRouteViewAttr{}, false
	}
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.Name), name) {
			return item, true
		}
	}
	return legacyRouteViewAttr{}, false
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func extractSummarySQL(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" || !strings.Contains(sqlText, "$View.") {
		return ""
	}
	matches := summaryJoinExpr.FindStringSubmatch(sqlText)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractRuleSettings(source *shape.Source) *ruleSettings {
	if source == nil || strings.TrimSpace(source.DQL) == "" {
		return &ruleSettings{}
	}
	matches := ruleHeaderExpr.FindStringSubmatch(source.DQL)
	if len(matches) < 2 {
		return &ruleSettings{}
	}
	rawJSON := strings.TrimSpace(matches[1])
	ret := &ruleSettings{}
	_ = json.Unmarshal([]byte(rawJSON), ret)
	return ret
}

func sourceSQLBaseDir(source *shape.Source) string {
	if source == nil {
		return ""
	}
	path := strings.TrimSpace(source.Path)
	if path == "" {
		return ""
	}
	base := strings.TrimSpace(filepath.Base(path))
	if base == "" {
		return ""
	}
	stem := strings.TrimSpace(strings.TrimSuffix(base, filepath.Ext(base)))
	if stem == "" || stem == "." || stem == string(filepath.Separator) {
		return ""
	}
	return stem
}

func sourceModule(source *shape.Source) string {
	return sourceModuleWithLayout(source, defaultCompilePathLayout())
}

func sourceModuleWithLayout(source *shape.Source, layout compilePathLayout) string {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	normalized := filepath.ToSlash(source.Path)
	marker := layout.dqlMarker
	if marker == "" {
		marker = defaultCompilePathLayout().dqlMarker
	}
	idx := strings.Index(normalized, marker)
	if idx == -1 {
		return ""
	}
	relative := strings.TrimPrefix(normalized[idx+len(marker):], "/")
	dir := strings.TrimSpace(filepath.ToSlash(filepath.Dir(relative)))
	if dir == "." || dir == "/" {
		return ""
	}
	return dir
}

func defaultSelectorNamespace(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			b.WriteByte(byte(strings.ToLower(string(ch))[0]))
		}
	}
	value := b.String()
	switch {
	case len(value) >= 2:
		return value[:2]
	case len(value) == 1:
		return value
	default:
		return ""
	}
}

func defaultSchemaType(name string, settings *ruleSettings, root bool) string {
	if root && settings != nil && strings.TrimSpace(settings.Name) != "" {
		return "*" + strings.TrimSpace(settings.Name) + "View"
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return "*" + toExportedTypeName(name) + "View"
}

func toExportedTypeName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '_' || r == '-' || r == ' ' || r == '.'
	})
	if len(parts) == 0 {
		return ""
	}
	var b strings.Builder
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			b.WriteString(part[1:])
		}
	}
	return b.String()
}

func shouldInferTable(item *plan.View) bool {
	if item == nil {
		return false
	}
	name := strings.TrimSpace(item.Name)
	table := strings.TrimSpace(item.Table)
	if table == "" {
		return true
	}
	if strings.HasPrefix(table, "(") {
		return true
	}
	if normalizedTemplatePlaceholderTable(table) {
		return true
	}
	return strings.EqualFold(name, table)
}

func normalizedTemplatePlaceholderTable(table string) bool {
	if table == "" {
		return false
	}
	parts := strings.Split(table, ".")
	if len(parts) < 3 {
		return false
	}
	for i := 0; i < len(parts)-1; i++ {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			return false
		}
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func inferTableFromSQL(sqlText string, source *shape.Source) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return ""
	}
	if expr := topLevelFromExpr(sqlText); expr != "" {
		if table := tableFromFromExpr(expr, source); table != "" {
			return table
		}
	}
	if table := pipeline.InferTableFromSQL(sqlText); table != "" {
		if !strings.EqualFold(table, "DQLView") {
			return table
		}
	}
	cleaned := embedExpr.ReplaceAllString(sqlText, " ")
	match := fromTableExpr.FindStringSubmatch(cleaned)
	if len(match) >= 2 {
		return strings.Trim(match[1], "`\"")
	}
	if table := inferFromEmbeddedSQL(sqlText, source); table != "" {
		return table
	}
	return ""
}

func inferFromEmbeddedSQL(sqlText string, source *shape.Source) string {
	matches := embedExpr.FindStringSubmatch(sqlText)
	if len(matches) < 2 {
		return ""
	}
	ref := strings.TrimSpace(matches[1])
	ref = strings.Trim(ref, `"'`)
	if ref == "" {
		return ""
	}
	resolved := resolveEmbedPath(source, ref)
	if resolved == "" {
		return ""
	}
	embedded, err := os.ReadFile(resolved)
	if err != nil {
		return ""
	}
	queryNode, _, err := pipeline.ParseSelectWithDiagnostic(string(embedded))
	if err != nil || queryNode == nil {
		fallback := fromTableExpr.FindStringSubmatch(string(embedded))
		if len(fallback) < 2 {
			return ""
		}
		return strings.Trim(fallback[1], "`\"")
	}
	_, table, err := pipeline.InferRoot(queryNode, "")
	if err != nil || strings.TrimSpace(table) == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(table), "DQLView") {
		return ""
	}
	return strings.Trim(table, "`\"")
}

func resolveEmbedPath(source *shape.Source, ref string) string {
	if filepath.IsAbs(ref) {
		return ref
	}
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	base := source.Path
	if fi, err := os.Stat(base); err == nil && fi.IsDir() {
		return filepath.Clean(filepath.Join(base, ref))
	}
	return filepath.Clean(filepath.Join(filepath.Dir(base), ref))
}

func inferTableFromSiblingSQL(viewName string, source *shape.Source) string {
	viewName = strings.TrimSpace(viewName)
	if viewName == "" || source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	sibling := filepath.Join(filepath.Dir(source.Path), viewName+".sql")
	data, err := os.ReadFile(sibling)
	if err != nil {
		sibling = filepath.Join(filepath.Dir(source.Path), strings.ToLower(viewName)+".sql")
		data, err = os.ReadFile(sibling)
	}
	if err != nil {
		return ""
	}
	return inferTableFromSQL(string(data), source)
}

func inferTableFromEmbedRef(source *shape.Source, ref string) string {
	ref = strings.Trim(strings.TrimSpace(ref), `"'`)
	if ref == "" {
		return ""
	}
	resolved := resolveEmbedPath(source, ref)
	if resolved == "" {
		return ""
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return ""
	}
	return pipeline.InferTableFromSQL(string(data))
}

func topLevelFromExpr(sqlText string) string {
	lower := strings.ToLower(sqlText)
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		}
		if inSingle || inDouble || inBacktick || depth != 0 {
			continue
		}
		if i+6 > len(sqlText) {
			break
		}
		if lower[i:i+4] != "from" {
			continue
		}
		if i > 0 {
			prev := lower[i-1]
			if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' {
				continue
			}
		}
		j := i + 4
		for j < len(sqlText) && (sqlText[j] == ' ' || sqlText[j] == '\n' || sqlText[j] == '\t' || sqlText[j] == '\r') {
			j++
		}
		if j >= len(sqlText) {
			return ""
		}
		if sqlText[j] == '(' {
			start := j
			d := 0
			for ; j < len(sqlText); j++ {
				if sqlText[j] == '(' {
					d++
				} else if sqlText[j] == ')' {
					d--
					if d == 0 {
						j++
						break
					}
				}
			}
			for j < len(sqlText) && (sqlText[j] == ' ' || sqlText[j] == '\n' || sqlText[j] == '\t' || sqlText[j] == '\r') {
				j++
			}
			for j < len(sqlText) {
				c := sqlText[j]
				if !(c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
					break
				}
				j++
			}
			return strings.TrimSpace(sqlText[start:j])
		}
		start := j
		for j < len(sqlText) {
			c := sqlText[j]
			if !(c == '_' || c == '.' || c == '/' || c == '{' || c == '}' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '$') {
				break
			}
			j++
		}
		return strings.TrimSpace(sqlText[start:j])
	}
	return ""
}

func tableFromFromExpr(fromExpr string, source *shape.Source) string {
	fromExpr = strings.TrimSpace(fromExpr)
	if fromExpr == "" {
		return ""
	}
	if strings.HasPrefix(fromExpr, "(") {
		if table := inferFromEmbeddedSQL(fromExpr, source); table != "" {
			return table
		}
		inner := fromExpr
		if idx := strings.LastIndex(inner, ")"); idx > 0 {
			inner = strings.TrimSpace(inner[1:idx])
		}
		return inferTableFromSQL(inner, source)
	}
	return strings.Trim(fromExpr, "`\"")
}

func inferConnector(item *plan.View, source *shape.Source) string {
	if item == nil {
		return ""
	}
	path := ""
	if source != nil {
		path = strings.ToLower(strings.ReplaceAll(source.Path, "\\", "/"))
	}
	table := strings.ToUpper(strings.TrimSpace(item.Table))
	switch {
	case strings.Contains(path, "/dql/system/"):
		return "system"
	case strings.HasPrefix(table, "CI_") || strings.Contains(table, ".CI_"):
		return "ci_ads"
	case strings.Contains(path, "/dql/ui/"):
		return "sitemgmt"
	case strings.Contains(table, "SITE"):
		return "sitemgmt"
	default:
		return ""
	}
}

func normalizeRootViewName(result *plan.Result, sourceName string, settings *ruleSettings) {
	if result == nil || len(result.Views) == 0 {
		return
	}
	root := result.Views[0]
	if root == nil {
		return
	}
	desired := sourceName
	if desired == "" {
		return
	}
	_ = settings
	current := strings.TrimSpace(root.Name)
	if current == "" {
		root.Name = desired
		root.Path = desired
		root.Holder = desired
		return
	}
	if strings.EqualFold(current, desired) {
		return
	}
	suspicious := map[string]bool{
		"and": true, "or": true, "status": true, "value": true, "watching": true,
	}
	if !suspicious[strings.ToLower(current)] {
		return
	}
	if result.ViewsByName != nil {
		delete(result.ViewsByName, root.Name)
	} else {
		result.ViewsByName = map[string]*plan.View{}
	}
	root.Name = desired
	root.Path = desired
	root.Holder = desired
	result.ViewsByName[root.Name] = root
}

func extractJoinEmbedRefs(sqlText string) map[string]string {
	result := map[string]string{}
	if strings.TrimSpace(sqlText) == "" {
		return result
	}
	for _, m := range joinEmbedExpr.FindAllStringSubmatch(sqlText, -1) {
		if len(m) < 3 {
			continue
		}
		ref := strings.TrimSpace(m[1])
		alias := strings.TrimSpace(m[2])
		if ref == "" || alias == "" {
			continue
		}
		result[alias] = ref
	}
	return result
}

func extractJoinSubqueryBodies(sqlText string) map[string]string {
	result := map[string]string{}
	if strings.TrimSpace(sqlText) == "" {
		return result
	}
	for _, m := range joinBodyExpr.FindAllStringSubmatch(sqlText, -1) {
		if len(m) < 3 {
			continue
		}
		body := strings.TrimSpace(m[1])
		alias := strings.TrimSpace(m[2])
		if body == "" || alias == "" {
			continue
		}
		result[alias] = body
	}
	return result
}
