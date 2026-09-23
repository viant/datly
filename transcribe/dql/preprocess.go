package dql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql/statement"
	xdocs "github.com/viant/xdatly/docs"
)

const (
	DiagnosticInvalidPackage = "DQL-DIR-PACKAGE"
	DiagnosticInvalidImport  = "DQL-DIR-IMPORT"
	DiagnosticInvalidSetting = "DQL-DIR-SETTING"
	DiagnosticInvalidDefine  = "DQL-DIR-DEFINE"
	DiagnosticParse          = "DQL-PARSE"
)

type SourceDiagnostic struct {
	Code    string
	Message string
	Offset  int
	End     int
}

// PreparedSource is the offset-preserving output of authored DQL
// preprocessing. Directives carries canonical route/settings/parameter data;
// SQL contains only executable source and Original is retained for diagnostics.
type PreparedSource struct {
	Original    string
	DirectSQL   string
	SQL         string
	TrimPrefix  int
	TypeContext *spec.TypeContext
	Directives  *DirectivePlan
	Statements  statement.Statements
	Diagnostics []SourceDiagnostic
}

// DirectivePlan is the normalized canonical product of one authored
// directive scan. Component assembly consumes this plan and never rescans the
// original source for routes, settings, or declarations.
type DirectivePlan struct {
	Static        *spec.StaticContent
	Documentation xdocs.Source
	Route         *RoutePlan
	Settings      *spec.Settings
	MCP           *spec.MCPExposure
	MCPOnly       bool
	Internal      bool
	Params        []*spec.Parameter
	Views         []*spec.View
	ParamSpans    map[string]SourceSpan
	ConstSpans    map[string]SourceSpan
	viewOptions   map[string]declarationViewOptions
}

type SourceSpan struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type RoutePlan struct {
	URI          string
	Methods      []string
	APIKeyHeader string
	APIKeyValue  string
}

func (p *PreparedSource) Err() error {
	if p == nil || len(p.Diagnostics) == 0 {
		return nil
	}
	return fmt.Errorf("%s", p.Diagnostics[0].Message)
}

// PrepareSource ports the original directive-masking preprocess stage using
// the shared parser-backed directive scanner. Masking preserves authored byte
// offsets and line endings; only outer SQL whitespace is trimmed.
func PrepareSource(source string) *PreparedSource {
	prepared := &PreparedSource{Original: source, Directives: &DirectivePlan{}}
	if source == "" {
		return prepared
	}
	blocks := extractDirectiveBlocks(source)
	mask := make([]bool, len(source))
	for _, block := range blocks {
		if shouldMaskBlock(block) {
			maskRange(mask, source, block.start, block.end)
		}
	}
	if directives, err := normalizeDirectivePlan(blocks); err != nil {
		if diagnostic, ok := DiagnosticForError(err); ok {
			prepared.Diagnostics = append(prepared.Diagnostics, diagnostic)
		} else {
			prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{
				Code: DiagnosticParse, Message: err.Error(), Offset: 0, End: 1,
			})
		}
	} else {
		prepared.Directives = directives
	}

	var typeContext spec.TypeContext
	offset := 0
	for _, line := range strings.SplitAfter(source, "\n") {
		trimmed := strings.TrimSpace(line)
		lineStart := offset
		lineEnd := offset + len(line)
		name, typeDirective := typeDirectiveName(trimmed)
		if typeDirective {
			start := firstNonSpaceOffset(line, offset)
			switch name {
			case "package":
				if pkg, ok := parsePackageLineDirective(trimmed); ok {
					if typeContext.PackagePath != "" && typeContext.PackagePath != pkg {
						prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{Code: DiagnosticInvalidPackage, Message: "conflicting #package destinations", Offset: start, End: lineEnd})
					}
					typeContext.PackagePath = pkg
					typeContext.DefaultPackage = pkg
				} else {
					prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{
						Code: DiagnosticInvalidPackage, Message: "invalid #package directive: expected #package('module/path')",
						Offset: start, End: start + len(strings.TrimRight(trimmed, "\r\n")),
					})
				}
			case "import":
				if alias, pkg, ok := parseImportLineDirective(trimmed); ok {
					typeContext.Imports = append(typeContext.Imports, spec.ImportSpec{Alias: alias, Package: pkg})
				} else {
					prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{
						Code: DiagnosticInvalidImport, Message: "invalid #import directive: expected #import('github.com/acme/pkg') or #import('alias','github.com/acme/pkg')",
						Offset: start, End: start + len(strings.TrimRight(trimmed, "\r\n")),
					})
				}
			}
			maskRange(mask, source, lineStart, lineEnd)
			offset += len(line)
			continue
		}
		if kind, _, ok := matchDirectiveAt(trimmed, 0); ok && !rangeMasked(mask, lineStart, lineEnd) {
			var code, directive string
			switch kind {
			case directiveKindSetting:
				code, directive = DiagnosticInvalidSetting, "setting"
			case directiveKindDefine:
				code, directive = DiagnosticInvalidDefine, "define"
			}
			if code != "" {
				start := firstNonSpaceOffset(line, offset)
				prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{
					Code: code, Message: "invalid #" + directive + " directive: unbalanced parentheses",
					Offset: start, End: start + len(strings.TrimRight(trimmed, "\r\n")),
				})
				// The closing boundary is unknown, so fail closed for the remaining
				// prepared SQL. Compilation already stops on this diagnostic.
				maskRange(mask, source, lineStart, len(source))
			}
		}
		offset += len(line)
	}
	if typeContext.DefaultPackage != "" || len(typeContext.Imports) > 0 {
		prepared.TypeContext = &typeContext
	}
	if len(prepared.Diagnostics) == 0 && prepared.Directives != nil {
		views, err := materializeDeclaredViews(prepared.Directives.Params, prepared.Directives.viewOptions, prepared.TypeContext)
		if err != nil {
			if diagnostic, ok := DiagnosticForError(err); ok {
				prepared.Diagnostics = append(prepared.Diagnostics, diagnostic)
			} else {
				prepared.Diagnostics = append(prepared.Diagnostics, SourceDiagnostic{
					Code: DiagnosticParse, Message: err.Error(), Offset: 0, End: 1,
				})
			}
		} else {
			prepared.Directives.Views = views
		}
	}

	masked := []byte(source)
	for index := range masked {
		if mask[index] && masked[index] != '\n' && masked[index] != '\r' {
			masked[index] = ' '
		}
	}
	maskedSource := string(masked)
	prepared.DirectSQL = maskedSource
	prepared.SQL = strings.TrimSpace(maskedSource)
	prepared.Statements = statement.Parse(prepared.SQL)
	if prepared.SQL != "" {
		prepared.TrimPrefix = strings.Index(maskedSource, prepared.SQL)
	}
	sort.SliceStable(prepared.Diagnostics, func(i, j int) bool {
		if prepared.Diagnostics[i].Offset == prepared.Diagnostics[j].Offset {
			return prepared.Diagnostics[i].End < prepared.Diagnostics[j].End
		}
		return prepared.Diagnostics[i].Offset < prepared.Diagnostics[j].Offset
	})
	return prepared
}

func normalizeDirectivePlan(blocks []directiveBlock) (*DirectivePlan, error) {
	route, routeErr := parseRouteDirective(blocks)
	settings, settingsErr := parseComponentSettings(blocks)
	params, paramSpans, viewOptions, declarationsErr := parseDeclarations(blocks)
	inferenceErr := inferRoutePathParams(route, &params)
	if err := earliestDirectiveError(routeErr, settingsErr, declarationsErr, inferenceErr); err != nil {
		return nil, err
	}
	if settings != nil && settings.MCP != nil && route != nil && len(route.Methods) != 1 {
		return nil, &sourceError{offset: settings.mcpSpan.Start, end: settings.mcpSpan.End, err: fmt.Errorf(
			"mcp directive is ambiguous across %d routes; declare exposure on one exact route", len(route.Methods))}
	}
	result := &DirectivePlan{Settings: toSpecSettings(settings), Params: params, ParamSpans: paramSpans, viewOptions: viewOptions}
	if settings != nil {
		result.MCP = settings.MCP.Clone()
		result.MCPOnly = settings.MCPOnly
		result.Internal = settings.Internal
		result.Documentation = settings.Documentation.Clone()
		result.Static = settings.Static.Clone()
	}
	if settings != nil && len(settings.constSpans) > 0 {
		result.ConstSpans = make(map[string]SourceSpan, len(settings.constSpans))
		for name, span := range settings.constSpans {
			result.ConstSpans[name] = span
		}
	}
	if route != nil {
		result.Route = &RoutePlan{
			URI: route.URI, Methods: append([]string(nil), route.Methods...),
			APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue,
		}
	}
	return result, nil
}

func inferRoutePathParams(route *routeDirective, params *[]*spec.Parameter) error {
	if route == nil || len(route.PathParams) == 0 || params == nil {
		return nil
	}
	for _, pathName := range route.PathParams {
		matched := false
		for _, param := range *params {
			if param == nil {
				continue
			}
			if param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(param.Source.Kind), "path") {
				sourceName := strings.TrimSpace(param.Source.Name)
				if sourceName == pathName {
					matched = true
					break
				}
				if strings.EqualFold(sourceName, pathName) {
					return &sourceError{offset: route.start, end: route.end, err: fmt.Errorf(
						"route path parameter %q must match declared path source %q exactly", pathName, sourceName)}
				}
			}
			if strings.EqualFold(strings.TrimSpace(param.Name), pathName) {
				return &sourceError{offset: route.start, end: route.end, err: fmt.Errorf(
					"route path parameter %q conflicts with declared %s source %q", pathName, param.Source.Kind, param.Source.Name)}
			}
		}
		if matched {
			continue
		}
		*params = append(*params, &spec.Parameter{
			Name: pathName,
			Source: spec.BindSource{
				Kind: "path",
				Name: pathName,
			},
			TypeExpr:    "string",
			Cardinality: "One",
		})
	}
	return nil
}

func rangeMasked(mask []bool, start, end int) bool {
	if start < 0 {
		start = 0
	}
	if end > len(mask) {
		end = len(mask)
	}
	for index := start; index < end; index++ {
		if mask[index] {
			return true
		}
	}
	return false
}

func shouldMaskBlock(block directiveBlock) bool {
	switch block.kind {
	case directiveKindSetting, directiveKindDefine:
		return true
	case directiveKindSet:
		return isDeclarationBlock(block) || isCubeSetting(block)
	default:
		return false
	}
}

func typeDirectiveName(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if len(line) < 2 || line[0] != '#' {
		return "", false
	}
	index := 1
	for index < len(line) && (isIdentifierPart(line[index])) {
		index++
	}
	name := strings.ToLower(line[1:index])
	return name, name == "package" || name == "import"
}

func maskRange(mask []bool, source string, start, end int) {
	if start < 0 {
		start = 0
	}
	if end > len(source) {
		end = len(source)
	}
	for index := start; index < end; index++ {
		if source[index] != '\n' && source[index] != '\r' {
			mask[index] = true
		}
	}
}

func firstNonSpaceOffset(line string, base int) int {
	for index := 0; index < len(line); index++ {
		switch line[index] {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return base + index
		}
	}
	return base
}
