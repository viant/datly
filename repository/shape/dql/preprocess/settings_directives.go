package preprocess

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/content"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/tagly/format/text"
)

var (
	metaDirectiveName        = map[string]bool{"meta": true}
	connectorDirectiveName   = map[string]bool{"connector": true}
	cacheDirectiveName       = map[string]bool{"cache": true}
	mcpDirectiveName         = map[string]bool{"mcp": true}
	routeDirectiveName       = map[string]bool{"route": true}
	constDirectiveName       = map[string]bool{"const": true}
	marshalDirectiveName     = map[string]bool{"marshal": true}
	unmarshalDirectiveName   = map[string]bool{"unmarshal": true}
	formatDirectiveName      = map[string]bool{"format": true}
	dateFormatDirectiveName  = map[string]bool{"date_format": true}
	caseFormatDirectiveName  = map[string]bool{"case_format": true}
	useTemplateDirectiveName = map[string]bool{"usetemplate": true}
	destDirectiveName        = map[string]bool{"dest": true}
	inputDestDirectiveName   = map[string]bool{"input_dest": true}
	outputDestDirectiveName  = map[string]bool{"output_dest": true}
	routerDestDirectiveName  = map[string]bool{"router_dest": true}
	inputTypeDirectiveName   = map[string]bool{"input_type": true}
	outputTypeDirectiveName  = map[string]bool{"output_type": true}
	cacheProviderExpr        = regexp.MustCompile(`(?i)\.withprovider\s*\(\s*['"]([^'"]+)['"]\s*\)`)
	cacheLocationExpr        = regexp.MustCompile(`(?i)\.withlocation\s*\(\s*['"]([^'"]+)['"]\s*\)`)
	cacheTTLMsExpr           = regexp.MustCompile(`(?i)\.withtimetolivems\s*\(\s*([0-9]+)\s*\)`)
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
		calls, parseErrors := scanDollarCallsStrict(input, metaDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirMeta, fullDQL, diagnosticOffset)
		values := parseMetaDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMeta, "invalid $meta directive", "expected: #settings($_ = $meta('relative/or/absolute/path'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.Meta = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$connector") {
		calls, parseErrors := scanDollarCallsStrict(input, connectorDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirConnector, fullDQL, diagnosticOffset)
		values := parseConnectorDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirConnector, "invalid $connector directive", "expected: #settings($_ = $connector('connector_name'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.DefaultConnector = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$cache") {
		calls, parseErrors := scanDollarCallsStrict(input, cacheDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirCache, fullDQL, diagnosticOffset)
		values := parseCacheDirectiveCalls(input, calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirCache, "invalid $cache directive", "expected: #settings($_ = $cache(true, '5m')) or #setting($_ = $cache('name').WithProvider('...').WithLocation('...').WithTimeToLiveMs(1000))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.Cache = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$mcp") {
		calls, parseErrors := scanDollarCallsStrict(input, mcpDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirMCP, fullDQL, diagnosticOffset)
		values := parseMCPDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMCP, "invalid $mcp directive", "expected: #settings($_ = $mcp('tool.name','description','docs/path.md'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.MCP = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$route") {
		calls, parseErrors := scanDollarCallsStrict(input, routeDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirRoute, fullDQL, diagnosticOffset)
		values := parseRouteDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirRoute, "invalid $route directive", "expected: #settings($_ = $route('/v1/api/path','GET','POST'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.Route = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$const") {
		calls, parseErrors := scanDollarCallsStrict(input, constDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirConst, fullDQL, diagnosticOffset)
		values := parseConstDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirConst, "invalid $const directive", "expected: #settings($_ = $const('Name','VALUE'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			if directives.Const == nil {
				directives.Const = map[string]string{}
			}
			for _, kv := range values {
				directives.Const[kv[0]] = kv[1]
			}
		}
	}
	if strings.Contains(lower, "$marshal") {
		calls, parseErrors := scanDollarCallsStrict(input, marshalDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirMarshal, fullDQL, diagnosticOffset)
		values := parseMarshalDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirMarshal, "invalid $marshal directive", "expected: #settings($_ = $marshal('application/json','pkg.Type'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.JSONMarshalType = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$unmarshal") {
		calls, parseErrors := scanDollarCallsStrict(input, unmarshalDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirUnmarshal, fullDQL, diagnosticOffset)
		values := parseUnmarshalDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirUnmarshal, "invalid $unmarshal directive", "expected: #settings($_ = $unmarshal('application/json','pkg.Type'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
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
		calls, parseErrors := scanDollarCallsStrict(input, formatDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirFormat, fullDQL, diagnosticOffset)
		values := parseFormatDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirFormat, "invalid $format directive", "expected: #settings($_ = $format('tabular_json'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.Format = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$date_format") {
		calls, parseErrors := scanDollarCallsStrict(input, dateFormatDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirDateFormat, fullDQL, diagnosticOffset)
		values := parseDateFormatDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirDateFormat, "invalid $date_format directive", "expected: #settings($_ = $date_format('2006-01-02'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.DateFormat = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$case_format") {
		calls, parseErrors := scanDollarCallsStrict(input, caseFormatDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirCaseFormat, fullDQL, diagnosticOffset)
		values := parseCaseFormatDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirCaseFormat, "invalid $case_format directive", "expected: #settings($_ = $case_format('lc'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.CaseFormat = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$usetemplate") {
		calls, parseErrors := scanDollarCallsStrict(input, useTemplateDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirUnsupported, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirUnsupported, "invalid $useTemplate directive", "expected: #settings($_ = $useTemplate('patch'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.TemplateType = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$dest") {
		calls, parseErrors := scanDollarCallsStrict(input, destDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirDest, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirDest, "invalid $dest directive", "expected: #settings($_ = $dest('file.go'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.Dest = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$input_dest") {
		calls, parseErrors := scanDollarCallsStrict(input, inputDestDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirInputDest, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirInputDest, "invalid $input_dest directive", "expected: #settings($_ = $input_dest('input.go'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.InputDest = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$output_dest") {
		calls, parseErrors := scanDollarCallsStrict(input, outputDestDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirOutputDest, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirOutputDest, "invalid $output_dest directive", "expected: #settings($_ = $output_dest('output.go'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.OutputDest = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$router_dest") {
		calls, parseErrors := scanDollarCallsStrict(input, routerDestDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirRouterDest, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirRouterDest, "invalid $router_dest directive", "expected: #settings($_ = $router_dest('router.go'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.RouterDest = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$input_type") {
		calls, parseErrors := scanDollarCallsStrict(input, inputTypeDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirInputType, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirInputType, "invalid $input_type directive", "expected: #settings($_ = $input_type('TypeName'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.InputType = values[len(values)-1]
		}
	}
	if strings.Contains(lower, "$output_type") {
		calls, parseErrors := scanDollarCallsStrict(input, outputTypeDirectiveName)
		diagnostics = appendDirectiveParseErrors(diagnostics, parseErrors, dqldiag.CodeDirOutputType, fullDQL, diagnosticOffset)
		values := parseSingleArgQuotedDirectiveCalls(calls)
		if len(values) == 0 {
			if len(calls) > 0 {
				diagnostics = append(diagnostics, directiveDiagnostic(dqldiag.CodeDirOutputType, "invalid $output_type directive", "expected: #settings($_ = $output_type('TypeName'))", fullDQL, lastDirectiveCallOffset(calls, diagnosticOffset)))
			}
		} else {
			directives.OutputType = values[len(values)-1]
		}
	}
	return diagnostics
}

func appendDirectiveParseErrors(diagnostics []*dqlshape.Diagnostic, parseErrors []directiveParseError, code, fullDQL string, diagnosticOffset int) []*dqlshape.Diagnostic {
	for _, parseErr := range parseErrors {
		message := "invalid directive syntax"
		if parseErr.name != "" {
			message = "invalid $" + parseErr.name + " directive"
		}
		diagnostics = append(diagnostics, directiveDiagnostic(code, message, "fix malformed directive call syntax", fullDQL, diagnosticOffset+parseErr.start))
	}
	return diagnostics
}

func lastDirectiveCallOffset(calls []directiveCall, diagnosticOffset int) int {
	if len(calls) == 0 {
		return diagnosticOffset
	}
	return diagnosticOffset + calls[len(calls)-1].start
}

func parseDestDirectives(input string) []string {
	calls := scanDollarCalls(input, destDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseInputDestDirectives(input string) []string {
	calls := scanDollarCalls(input, inputDestDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseOutputDestDirectives(input string) []string {
	calls := scanDollarCalls(input, outputDestDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseRouterDestDirectives(input string) []string {
	calls := scanDollarCalls(input, routerDestDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseInputTypeDirectives(input string) []string {
	calls := scanDollarCalls(input, inputTypeDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseOutputTypeDirectives(input string) []string {
	calls := scanDollarCalls(input, outputTypeDirectiveName)
	return parseSingleArgQuotedDirectiveCalls(calls)
}

func parseSingleArgQuotedDirectiveCalls(calls []directiveCall) []string {
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

func parseMetaDirectives(input string) []string {
	calls := scanDollarCalls(input, metaDirectiveName)
	return parseMetaDirectiveCalls(calls)
}

func parseMetaDirectiveCalls(calls []directiveCall) []string {
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
	return parseConnectorDirectiveCalls(calls)
}

func parseConnectorDirectiveCalls(calls []directiveCall) []string {
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
	return parseCacheDirectiveCalls(input, calls)
}

func parseCacheDirectiveCalls(input string, calls []directiveCall) []*dqlshape.CacheDirective {
	result := make([]*dqlshape.CacheDirective, 0, len(calls))
	for _, call := range calls {
		if len(call.args) == 0 || len(call.args) > 2 {
			continue
		}
		firstArg := strings.TrimSpace(call.args[0])
		if strings.EqualFold(firstArg, "true") || strings.EqualFold(firstArg, "false") {
			ttl := ""
			if len(call.args) == 2 {
				value, ok := parseQuotedLiteral(call.args[1])
				if !ok {
					continue
				}
				ttl = strings.TrimSpace(value)
			}
			result = append(result, &dqlshape.CacheDirective{
				Enabled: strings.EqualFold(firstArg, "true"),
				TTL:     ttl,
			})
			continue
		}
		name, ok := parseQuotedLiteral(firstArg)
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		tail := ""
		if call.end > 0 && call.end <= len(input) {
			tail = input[call.end:]
		}
		cacheDirective := &dqlshape.CacheDirective{
			Enabled: true,
			Name:    name,
		}
		if match := cacheProviderExpr.FindStringSubmatch(tail); len(match) > 1 {
			cacheDirective.Provider = strings.TrimSpace(match[1])
		}
		if match := cacheLocationExpr.FindStringSubmatch(tail); len(match) > 1 {
			cacheDirective.Location = strings.TrimSpace(match[1])
		}
		if match := cacheTTLMsExpr.FindStringSubmatch(tail); len(match) > 1 {
			if ttlMs, err := strconv.Atoi(strings.TrimSpace(match[1])); err == nil && ttlMs > 0 {
				cacheDirective.TimeToLiveMs = ttlMs
			}
		}
		if cacheDirective.Provider == "" || cacheDirective.Location == "" || cacheDirective.TimeToLiveMs <= 0 {
			continue
		}
		if len(call.args) == 2 {
			value, ok := parseQuotedLiteral(call.args[1])
			if ok {
				cacheDirective.TTL = strings.TrimSpace(value)
			}
		}
		if cacheDirective.TTL == "" {
			cacheDirective.TTL = strconv.Itoa(cacheDirective.TimeToLiveMs) + "ms"
		}
		result = append(result, cacheDirective)
	}
	return result
}

func parseMCPDirectives(input string) []*dqlshape.MCPDirective {
	calls := scanDollarCalls(input, mcpDirectiveName)
	return parseMCPDirectiveCalls(calls)
}

func parseMCPDirectiveCalls(calls []directiveCall) []*dqlshape.MCPDirective {
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
	return parseRouteDirectiveCalls(calls)
}

func parseRouteDirectiveCalls(calls []directiveCall) []*dqlshape.RouteDirective {
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
	return parseMarshalDirectiveCalls(calls)
}

func parseMarshalDirectiveCalls(calls []directiveCall) []string {
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
	return parseUnmarshalDirectiveCalls(calls)
}

func parseUnmarshalDirectiveCalls(calls []directiveCall) []unmarshalDirectiveValue {
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
	return parseFormatDirectiveCalls(calls)
}

func parseFormatDirectiveCalls(calls []directiveCall) []string {
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
	return parseDateFormatDirectiveCalls(calls)
}

func parseDateFormatDirectiveCalls(calls []directiveCall) []string {
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
	return parseCaseFormatDirectiveCalls(calls)
}

func parseCaseFormatDirectiveCalls(calls []directiveCall) []string {
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

func parseConstDirectives(input string) [][2]string {
	calls := scanDollarCalls(input, constDirectiveName)
	return parseConstDirectiveCalls(calls)
}

func parseConstDirectiveCalls(calls []directiveCall) [][2]string {
	var result [][2]string
	for _, call := range calls {
		if len(call.args) != 2 {
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
		value, ok := parseQuotedLiteral(call.args[1])
		if !ok {
			continue
		}
		result = append(result, [2]string{name, strings.TrimSpace(value)})
	}
	return result
}
