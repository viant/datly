package compile

// enrich.go — per-view enrichment passes applied after DQL compilation.
// Table inference helpers live in enrich_table.go; low-level text scanning
// primitives live in enrich_text.go.

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
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
	body, ok := findSummaryJoinBody(sqlText)
	if !ok {
		return ""
	}
	return strings.TrimSpace(body)
}

func extractRuleSettings(source *shape.Source, directives *dqlshape.Directives) *ruleSettings {
	if source == nil || strings.TrimSpace(source.DQL) == "" {
		return &ruleSettings{}
	}
	ret := &ruleSettings{}
	if rawJSON, ok := extractLeadingRuleHeaderJSON(source.DQL); ok {
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

// extractJoinEmbedRefs builds a map of view-alias → embed-path for every
// JOIN(${embed:path}) alias clause found in sqlText.
func extractJoinEmbedRefs(sqlText string) map[string]string {
	result := map[string]string{}
	if strings.TrimSpace(sqlText) == "" {
		return result
	}
	for _, item := range scanJoinSubqueries(sqlText) {
		ref, ok := parseJoinEmbedRef(item.body)
		if !ok || ref == "" || item.alias == "" {
			continue
		}
		result[item.alias] = ref
	}
	return result
}

// extractJoinSubqueryBodies builds a map of view-alias → subquery-body for
// every JOIN(body) alias clause found in sqlText.
func extractJoinSubqueryBodies(sqlText string) map[string]string {
	result := map[string]string{}
	if strings.TrimSpace(sqlText) == "" {
		return result
	}
	for _, item := range scanJoinSubqueries(sqlText) {
		body := strings.TrimSpace(item.body)
		if body == "" || item.alias == "" {
			continue
		}
		result[item.alias] = body
	}
	return result
}
