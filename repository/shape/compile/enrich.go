package compile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
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

type parityEnrichmentContext struct {
	source             *shape.Source
	settings           *ruleSettings
	baseDir            string
	module             string
	sourceName         string
	joinEmbedRefs      map[string]string
	joinSubqueryBodies map[string]string
}

func applySourceParityEnrichment(result *plan.Result, source *shape.Source) {
	applySourceParityEnrichmentWithLayout(result, source, defaultCompilePathLayout())
}

func applySourceParityEnrichmentWithLayout(result *plan.Result, source *shape.Source, layout compilePathLayout) {
	if result == nil || len(result.Views) == 0 {
		return
	}
	ctx := buildParityEnrichmentContext(result, source, layout)
	for idx, item := range result.Views {
		if item == nil {
			continue
		}
		applyViewDefaults(item, idx == 0, ctx)
		applyTableInference(item, ctx)
		applyConnectorInference(item, ctx)
		applySummaryInference(item, ctx)
	}
	if source != nil && strings.TrimSpace(source.Path) != "" {
		normalizeRootViewName(result, ctx.sourceName)
	}
}

func buildParityEnrichmentContext(result *plan.Result, source *shape.Source, layout compilePathLayout) *parityEnrichmentContext {
	ctx := &parityEnrichmentContext{
		source:             source,
		settings:           extractRuleSettings(source, result.Directives),
		baseDir:            sourceSQLBaseDir(source),
		module:             sourceModuleWithLayout(source, layout),
		sourceName:         pipeline.SanitizeName(source.Name),
		joinEmbedRefs:      map[string]string{},
		joinSubqueryBodies: map[string]string{},
	}
	if len(result.Views) == 0 || result.Views[0] == nil {
		return ctx
	}
	sqlForJoinExtract := result.Views[0].SQL
	if source != nil && strings.TrimSpace(source.DQL) != "" {
		sqlForJoinExtract = source.DQL
	}
	ctx.joinEmbedRefs = extractJoinEmbedRefs(sqlForJoinExtract)
	ctx.joinSubqueryBodies = extractJoinSubqueryBodies(sqlForJoinExtract)
	return ctx
}

func applyViewDefaults(item *plan.View, root bool, ctx *parityEnrichmentContext) {
	if item == nil || ctx == nil {
		return
	}
	if item.SQLURI == "" && ctx.baseDir != "" {
		item.SQLURI = ctx.baseDir + "/" + item.Name + ".sql"
	}
	if item.Module == "" {
		item.Module = ctx.module
	}
	if item.SelectorNamespace == "" {
		item.SelectorNamespace = defaultSelectorNamespace(item.Name)
	}
	if item.SchemaType == "" {
		item.SchemaType = defaultSchemaType(item.Name, ctx.settings, root)
	}
}

func applyTableInference(item *plan.View, ctx *parityEnrichmentContext) {
	if item == nil || ctx == nil {
		return
	}
	if shouldInferTable(item) {
		candidateSQL := item.SQL
		if strings.TrimSpace(candidateSQL) == "" {
			candidateSQL = item.Table
		}
		if table := inferTableFromSQL(candidateSQL, ctx.source); table != "" {
			item.Table = table
		}
	}
	if strings.HasPrefix(strings.TrimSpace(item.Table), "(") || normalizedTemplatePlaceholderTable(strings.TrimSpace(item.Table)) {
		if ref, ok := ctx.joinEmbedRefs[item.Name]; ok {
			if table := inferTableFromEmbedRef(ctx.source, ref); table != "" {
				item.Table = table
			}
		}
		if body, ok := ctx.joinSubqueryBodies[item.Name]; ok {
			if table := inferTableFromSQL(body, ctx.source); table != "" {
				item.Table = table
			}
		}
		if table := inferTableFromSiblingSQL(item.Name, ctx.source); table != "" {
			item.Table = table
		}
	}
}

func applyConnectorInference(item *plan.View, ctx *parityEnrichmentContext) {
	if item == nil || ctx == nil || item.Connector != "" {
		return
	}
	if ctx.settings != nil && ctx.settings.Connector != "" {
		item.Connector = ctx.settings.Connector
	}
	if item.Connector == "" && ctx.source != nil && strings.TrimSpace(ctx.source.Connector) != "" {
		item.Connector = strings.TrimSpace(ctx.source.Connector)
	}
	if item.Connector == "" {
		item.Connector = inferConnector(item, ctx.source)
	}
}

func applySummaryInference(item *plan.View, ctx *parityEnrichmentContext) {
	if item == nil || ctx == nil || item.Summary != "" {
		return
	}
	item.Summary = extractSummarySQL(item.SQL)
	if item.Summary == "" && ctx.source != nil {
		item.Summary = extractSummarySQL(ctx.source.DQL)
	}
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

func extractRuleSettings(source *shape.Source, directives *dqlshape.Directives) *ruleSettings {
	if source == nil || strings.TrimSpace(source.DQL) == "" {
		return &ruleSettings{}
	}
	ret := &ruleSettings{}
	matches := ruleHeaderExpr.FindStringSubmatch(source.DQL)
	if len(matches) >= 2 {
		rawJSON := strings.TrimSpace(matches[1])
		_ = json.Unmarshal([]byte(rawJSON), ret)
	}
	if directives != nil && directives.Route != nil {
		if uri := strings.TrimSpace(directives.Route.URI); uri != "" {
			ret.URI = uri
		}
		if len(directives.Route.Methods) > 0 {
			ret.Method = strings.Join(directives.Route.Methods, ",")
		}
	}
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

func normalizeRootViewName(result *plan.Result, sourceName string) {
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
