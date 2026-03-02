package preprocess

import (
	"net/http"
	"strings"

	"github.com/viant/datly/repository/content"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/tagly/format/text"
)

var (
	metaDirectiveName       = map[string]bool{"meta": true}
	connectorDirectiveName  = map[string]bool{"connector": true}
	cacheDirectiveName      = map[string]bool{"cache": true}
	mcpDirectiveName        = map[string]bool{"mcp": true}
	routeDirectiveName      = map[string]bool{"route": true}
	marshalDirectiveName    = map[string]bool{"marshal": true}
	unmarshalDirectiveName  = map[string]bool{"unmarshal": true}
	formatDirectiveName     = map[string]bool{"format": true}
	dateFormatDirectiveName = map[string]bool{"date_format": true}
	caseFormatDirectiveName = map[string]bool{"case_format": true}
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
	calls := scanDollarCalls(input, metaDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 1 {
			continue
		}
		value, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseConnectorDirectives(input string) []string {
	calls := scanDollarCalls(input, connectorDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 1 {
			continue
		}
		value, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseCacheDirectives(input string) []*dqlshape.CacheDirective {
	calls := scanDollarCalls(input, cacheDirectiveName)
	result := make([]*dqlshape.CacheDirective, 0, len(calls))
	for _, call := range calls {
		if len(call.args) == 0 || len(call.args) > 2 {
			continue
		}
		enabledRaw := strings.TrimSpace(call.args[0])
		var enabled bool
		switch {
		case strings.EqualFold(enabledRaw, "true"):
			enabled = true
		case strings.EqualFold(enabledRaw, "false"):
			enabled = false
		default:
			continue
		}
		ttl := ""
		if len(call.args) == 2 {
			value, ok := parseQuotedLiteral(call.args[1])
			if !ok {
				continue
			}
			ttl = strings.TrimSpace(value)
		}
		result = append(result, &dqlshape.CacheDirective{Enabled: enabled, TTL: ttl})
	}
	return result
}

func parseMCPDirectives(input string) []*dqlshape.MCPDirective {
	calls := scanDollarCalls(input, mcpDirectiveName)
	result := make([]*dqlshape.MCPDirective, 0, len(calls))
	for _, call := range calls {
		if len(call.args) < 1 || len(call.args) > 3 {
			continue
		}
		name, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		description := ""
		if len(call.args) > 1 {
			value, ok := parseQuotedLiteral(call.args[1])
			if !ok {
				continue
			}
			description = strings.TrimSpace(value)
		}
		descriptionPath := ""
		if len(call.args) > 2 {
			value, ok := parseQuotedLiteral(call.args[2])
			if !ok {
				continue
			}
			descriptionPath = strings.TrimSpace(value)
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
	calls := scanDollarCalls(input, routeDirectiveName)
	result := make([]*dqlshape.RouteDirective, 0, len(calls))
	for _, call := range calls {
		if len(call.args) == 0 {
			continue
		}
		uri, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		uri = strings.TrimSpace(uri)
		if !strings.HasPrefix(uri, "/") {
			continue
		}
		methodsRaw := make([]string, 0, len(call.args)-1)
		for _, arg := range call.args[1:] {
			method, ok := parseQuotedLiteral(arg)
			if !ok {
				methodsRaw = nil
				break
			}
			methodsRaw = append(methodsRaw, method)
		}
		if methodsRaw == nil {
			continue
		}
		methods, ok := normalizeHTTPMethods(methodsRaw)
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
	calls := scanDollarCalls(input, marshalDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 2 {
			continue
		}
		mimeType, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		mimeType = strings.ToLower(strings.TrimSpace(mimeType))
		if mimeType != content.JSONContentType {
			continue
		}
		typeName, ok := parseQuotedLiteral(call.args[1])
		if !ok {
			continue
		}
		if typeName = strings.TrimSpace(typeName); typeName != "" {
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
	calls := scanDollarCalls(input, unmarshalDirectiveName)
	result := make([]unmarshalDirectiveValue, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 2 {
			continue
		}
		mimeType, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		typeName, ok := parseQuotedLiteral(call.args[1])
		if !ok {
			continue
		}
		mimeType = strings.ToLower(strings.TrimSpace(mimeType))
		typeName = strings.TrimSpace(typeName)
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
	calls := scanDollarCalls(input, formatDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 1 {
			continue
		}
		raw, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		raw = strings.ToLower(strings.TrimSpace(raw))
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
	calls := scanDollarCalls(input, dateFormatDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 1 {
			continue
		}
		value, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func parseCaseFormatDirectives(input string) []string {
	calls := scanDollarCalls(input, caseFormatDirectiveName)
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		if len(call.args) != 1 {
			continue
		}
		value, ok := parseQuotedLiteral(call.args[0])
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
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
