package preprocess

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/viant/datly/repository/content"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/tagly/format/text"
)

var (
	metaDirective       = regexp.MustCompile(`(?i)\$meta\s*\(\s*['\"]([^'\"]+)['\"]\s*\)`)
	connectorDirective  = regexp.MustCompile(`(?i)\$connector\s*\(\s*['\"]([^'\"]+)['\"]\s*\)`)
	cacheDirective      = regexp.MustCompile(`(?i)\$cache\s*\(\s*(true|false)\s*(?:,\s*['\"]([^'\"]+)['\"]\s*)?\)`)
	mcpDirective        = regexp.MustCompile(`(?i)\$mcp\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*['\"]([^'\"]*)['\"]\s*)?(?:,\s*['\"]([^'\"]*)['\"]\s*)?\)`)
	routeDirective      = regexp.MustCompile(`(?i)\$route\s*\(([^)]*)\)`)
	marshalDirective    = regexp.MustCompile(`(?i)\$marshal\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)`)
	unmarshalDirective  = regexp.MustCompile(`(?i)\$unmarshal\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)`)
	formatDirective     = regexp.MustCompile(`(?i)\$format\s*\(\s*['\"]([^'\"]+)['\"]\s*\)`)
	dateFormatDirective = regexp.MustCompile(`(?i)\$date_format\s*\(\s*['\"]([^'\"]+)['\"]\s*\)`)
	caseFormatDirective = regexp.MustCompile(`(?i)\$case_format\s*\(\s*['\"]([^'\"]+)['\"]\s*\)`)
	quotedArgDirective  = regexp.MustCompile(`['\"]([^'\"]*)['\"]`)
)

func parseSettingsDirectives(input, fullDQL string, diagnosticOffset int, directives *dqlshape.Directives) []*dqlshape.Diagnostic {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	var diagnostics []*dqlshape.Diagnostic
	lower := strings.ToLower(input)
	if strings.Contains(lower, "$package") || strings.Contains(lower, "$import") {
		diagnostics = append(diagnostics, directiveDiagnostic(
			dqldiag.CodeDirUnsupported,
			"type-context directives are not allowed in #settings",
			"use #package('module/path') and #import('alias','github.com/acme/pkg')",
			fullDQL,
			diagnosticOffset,
		))
	}
	if strings.Contains(lower, "$meta") {
		values := parseMetaDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMeta, "invalid $meta directive", "expected: #settings($_ = $meta('relative/or/absolute/path'))", fullDQL, diagnosticOffset))
		} else {
			directives.Meta = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$connector") {
		values := parseConnectorDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirConnector, "invalid $connector directive", "expected: #settings($_ = $connector('connector_name'))", fullDQL, diagnosticOffset))
		} else {
			directives.DefaultConnector = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$cache") {
		values := parseCacheDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirCache, "invalid $cache directive", "expected: #settings($_ = $cache(true, '5m'))", fullDQL, diagnosticOffset))
		} else {
			directives.Cache = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$mcp") {
		values := parseMCPDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMCP, "invalid $mcp directive", "expected: #settings($_ = $mcp('tool.name','description','docs/path.md'))", fullDQL, diagnosticOffset))
		} else {
			directives.MCP = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$route") {
		values := parseRouteDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirRoute, "invalid $route directive", "expected: #settings($_ = $route('/v1/api/path','GET','POST'))", fullDQL, diagnosticOffset))
		} else {
			directives.Route = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$marshal") {
		values := parseMarshalDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMarshal, "invalid $marshal directive", "expected: #settings($_ = $marshal('application/json','pkg.Type'))", fullDQL, diagnosticOffset))
		} else {
			directives.JSONMarshalType = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$unmarshal") {
		values := parseUnmarshalDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirUnmarshal, "invalid $unmarshal directive", "expected: #settings($_ = $unmarshal('application/json','pkg.Type'))", fullDQL, diagnosticOffset))
		} else {
			last := values[len(values)-1]
			if last.JSONType != "" {
				directives.JSONUnmarshalType = last.JSONType
			}
			if last.XMLType != "" {
				directives.XMLUnmarshalType = last.XMLType
			}
		}
	}
	if strings.Contains(lower, "$format") {
		values := parseFormatDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirFormat, "invalid $format directive", "expected: #settings($_ = $format('tabular_json'))", fullDQL, diagnosticOffset))
		} else {
			directives.Format = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$date_format") {
		values := parseDateFormatDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirDateFormat, "invalid $date_format directive", "expected: #settings($_ = $date_format('2006-01-02'))", fullDQL, diagnosticOffset))
		} else {
			directives.DateFormat = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$case_format") {
		values := parseCaseFormatDirectives(input)
		if len(values) == 0 {
			diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirCaseFormat, "invalid $case_format directive", "expected: #settings($_ = $case_format('lc'))", fullDQL, diagnosticOffset))
		} else {
			directives.CaseFormat = values[len(values)-1]
		}
	}
	return diagnostics
}

func parseMetaDirectives(input string) []string {
	matches := metaDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		if value := strings.TrimSpace(match[1]); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseConnectorDirectives(input string) []string {
	matches := connectorDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		if value := strings.TrimSpace(match[1]); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseCacheDirectives(input string) []*dqlshape.CacheDirective {
	matches := cacheDirective.FindAllStringSubmatch(input, -1)
	result := make([]*dqlshape.CacheDirective, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		enabled := strings.EqualFold(strings.TrimSpace(match[1]), "true")
		ttl := ""
		if len(match) > 2 {
			ttl = strings.TrimSpace(match[2])
		}
		result = append(result, &dqlshape.CacheDirective{Enabled: enabled, TTL: ttl})
	}
	return result
}

func parseMCPDirectives(input string) []*dqlshape.MCPDirective {
	matches := mcpDirective.FindAllStringSubmatch(input, -1)
	result := make([]*dqlshape.MCPDirective, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := strings.TrimSpace(match[1])
		if name == "" {
			continue
		}
		description := ""
		if len(match) > 2 {
			description = strings.TrimSpace(match[2])
		}
		descriptionPath := ""
		if len(match) > 3 {
			descriptionPath = strings.TrimSpace(match[3])
		}
		result = append(result, &dqlshape.MCPDirective{
			Name:            name,
			Description:     description,
			DescriptionPath: descriptionPath,
		})
	}
	return result
}

func parseRouteDirectives(input string) []*dqlshape.RouteDirective {
	matches := routeDirective.FindAllStringSubmatch(input, -1)
	result := make([]*dqlshape.RouteDirective, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		args := parseQuotedArgs(match[1])
		if len(args) == 0 {
			continue
		}
		uri := strings.TrimSpace(args[0])
		if !strings.HasPrefix(uri, "/") {
			continue
		}
		methods, ok := normalizeHTTPMethods(args[1:])
		if !ok {
			continue
		}
		result = append(result, &dqlshape.RouteDirective{
			URI:     uri,
			Methods: methods,
		})
	}
	return result
}

func parseQuotedArgs(input string) []string {
	matches := quotedArgDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		result = append(result, strings.TrimSpace(match[1]))
	}
	return result
}

func normalizeHTTPMethods(input []string) ([]string, bool) {
	if len(input) == 0 {
		return nil, true
	}
	valid := map[string]bool{
		http.MethodGet:     true,
		http.MethodPost:    true,
		http.MethodPut:     true,
		http.MethodPatch:   true,
		http.MethodDelete:  true,
		http.MethodHead:    true,
		http.MethodOptions: true,
		http.MethodTrace:   true,
		http.MethodConnect: true,
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(input))
	for _, item := range input {
		method := strings.ToUpper(strings.TrimSpace(item))
		if method == "" {
			return nil, false
		}
		if !valid[method] {
			return nil, false
		}
		if seen[method] {
			continue
		}
		seen[method] = true
		result = append(result, method)
	}
	return result, true
}

func parseMarshalDirectives(input string) []string {
	matches := marshalDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		mimeType := strings.ToLower(strings.TrimSpace(match[1]))
		if mimeType != content.JSONContentType {
			continue
		}
		if typeName := strings.TrimSpace(match[2]); typeName != "" {
			result = append(result, typeName)
		}
	}
	return result
}

type unmarshalDirectiveValue struct {
	JSONType string
	XMLType  string
}

func parseUnmarshalDirectives(input string) []unmarshalDirectiveValue {
	matches := unmarshalDirective.FindAllStringSubmatch(input, -1)
	result := make([]unmarshalDirectiveValue, 0, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		mimeType := strings.ToLower(strings.TrimSpace(match[1]))
		typeName := strings.TrimSpace(match[2])
		if typeName == "" {
			continue
		}
		value := unmarshalDirectiveValue{}
		switch mimeType {
		case content.JSONContentType:
			value.JSONType = typeName
		case content.XMLContentType:
			value.XMLType = typeName
		default:
			continue
		}
		result = append(result, value)
	}
	return result
}

func parseFormatDirectives(input string) []string {
	matches := formatDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		raw := strings.ToLower(strings.TrimSpace(match[1]))
		switch raw {
		case "tabular_json":
			result = append(result, content.JSONDataFormatTabular)
		case content.JSONFormat, content.XMLFormat, content.CSVFormat, content.JSONDataFormatTabular:
			result = append(result, raw)
		}
	}
	return result
}

func parseDateFormatDirectives(input string) []string {
	matches := dateFormatDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		if value := strings.TrimSpace(match[1]); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseCaseFormatDirectives(input string) []string {
	matches := caseFormatDirective.FindAllStringSubmatch(input, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		value := strings.TrimSpace(match[1])
		if value == "" {
			continue
		}
		if !text.NewCaseFormat(value).IsDefined() {
			continue
		}
		result = append(result, value)
	}
	return result
}
