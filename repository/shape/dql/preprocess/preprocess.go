package preprocess

import (
	"strings"

	dqlopt "github.com/viant/datly/repository/shape/dql/optimize"
	dqlsanitize "github.com/viant/datly/repository/shape/dql/sanitize"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

type Result struct {
	Original    string
	DirectSQL   string
	Optimized   string
	SQL         string
	TypeCtx     *typectx.Context
	Directives  *dqlshape.Directives
	Mapper      *Mapper
	Diagnostics []*dqlshape.Diagnostic
}

// Extract parses directives and returns SQL with directive lines masked to preserve offsets.
func Extract(dql string) (string, *typectx.Context, *dqlshape.Directives, []*dqlshape.Diagnostic) {
	sql, ctx, directives, diags := extractSQLAndContext(dql)
	return sql, normalizeTypeContext(ctx), normalizeDirectives(directives), diags
}

func Prepare(dql string) *Result {
	ret := &Result{Original: dql}
	sql, typeCtx, directives, dirDiags := Extract(dql)
	ret.DirectSQL = stripDecorators(sql)
	ret.TypeCtx = typeCtx
	ret.Directives = directives
	ret.Diagnostics = append(ret.Diagnostics, dirDiags...)
	if strings.TrimSpace(ret.DirectSQL) == "" {
		return ret
	}
	optimized, optDiags := dqlopt.Rewrite(ret.DirectSQL)
	ret.Diagnostics = append(ret.Diagnostics, optDiags...)
	ret.Optimized = optimized
	sanitized := dqlsanitize.Rewrite(optimized, dqlsanitize.Options{
		Declared: dqlsanitize.Declared(optimized),
	})
	ret.SQL = sanitized.SQL
	ret.Mapper = newMapper(len(optimized), sanitized.Patches, sanitized.TrimPrefix, dql)
	return ret
}

func stripDecorators(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	lines := strings.Split(sql, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if isStandaloneDecoratorLine(line) {
			continue
		}
		filtered = append(filtered, line)
	}
	return cleanupLineCommaArtifacts(filtered)
}

func isStandaloneDecoratorLine(line string) bool {
	trimmed := strings.TrimSpace(strings.TrimSuffix(line, ","))
	if trimmed == "" {
		return false
	}
	open := strings.Index(trimmed, "(")
	close := strings.LastIndex(trimmed, ")")
	if open <= 0 || close <= open {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(trimmed[:open]))
	switch name {
	case "use_connector", "allow_nulls", "allownulls", "tag", "cast", "required", "cardinality", "set_limit":
		return true
	default:
		return false
	}
}

func cleanupLineCommaArtifacts(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(result) > 0 && strings.HasPrefix(strings.ToLower(trimmed), "from ") {
			prev := strings.TrimRight(result[len(result)-1], " \t")
			prev = strings.TrimSuffix(prev, ",")
			result[len(result)-1] = prev
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func normalizeTypeContext(ctx *typectx.Context) *typectx.Context {
	if ctx == nil {
		return nil
	}
	if ctx.DefaultPackage == "" && len(ctx.Imports) == 0 {
		return nil
	}
	return ctx
}

func normalizeDirectives(input *dqlshape.Directives) *dqlshape.Directives {
	if input == nil {
		return nil
	}
	ret := &dqlshape.Directives{
		Meta:              strings.TrimSpace(input.Meta),
		DefaultConnector:  strings.TrimSpace(input.DefaultConnector),
		JSONMarshalType:   strings.TrimSpace(input.JSONMarshalType),
		JSONUnmarshalType: strings.TrimSpace(input.JSONUnmarshalType),
		XMLUnmarshalType:  strings.TrimSpace(input.XMLUnmarshalType),
		Format:            strings.TrimSpace(input.Format),
		DateFormat:        strings.TrimSpace(input.DateFormat),
		CaseFormat:        strings.TrimSpace(input.CaseFormat),
	}
	if input.Cache != nil {
		ret.Cache = &dqlshape.CacheDirective{
			Enabled: input.Cache.Enabled,
			TTL:     strings.TrimSpace(input.Cache.TTL),
		}
	}
	if input.MCP != nil {
		ret.MCP = &dqlshape.MCPDirective{
			Name:            strings.TrimSpace(input.MCP.Name),
			Description:     strings.TrimSpace(input.MCP.Description),
			DescriptionPath: strings.TrimSpace(input.MCP.DescriptionPath),
		}
	}
	if input.Route != nil {
		normalizedMethods := make([]string, 0, len(input.Route.Methods))
		for _, method := range input.Route.Methods {
			if method = strings.TrimSpace(method); method != "" {
				normalizedMethods = append(normalizedMethods, method)
			}
		}
		ret.Route = &dqlshape.RouteDirective{
			URI:     strings.TrimSpace(input.Route.URI),
			Methods: normalizedMethods,
		}
	}
	if ret.Meta == "" && ret.DefaultConnector == "" && ret.Cache == nil && ret.MCP == nil && ret.Route == nil &&
		ret.JSONMarshalType == "" && ret.JSONUnmarshalType == "" && ret.XMLUnmarshalType == "" && ret.Format == "" &&
		ret.DateFormat == "" && ret.CaseFormat == "" {
		return nil
	}
	return ret
}
