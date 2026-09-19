package readerbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
)

type projectionSource struct {
	start, end int
	items      []dql.SourceSpan
}

func inspectFunctions(source string) ([]FunctionOccurrence, error) {
	projection, err := outerProjection(source)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	var result []FunctionOccurrence
	for _, span := range projection.items {
		raw := strings.TrimSpace(source[span.Start:span.End])
		call, err := sqlparser.ParseCallExpr(raw)
		if err != nil || call == nil {
			continue
		}
		name := strings.TrimSpace(sqlparser.Stringify(call.X))
		if name == "" {
			continue
		}
		args := make([]string, 0, len(call.Args))
		for _, arg := range call.Args {
			args = append(args, strings.TrimSpace(sqlparser.Stringify(arg)))
		}
		if strings.EqualFold(name, "cast") {
			if cast, castErr := sqlparser.CastExpression(call); castErr == nil && cast != nil {
				args = []string{cast.Operand, cast.Type}
			}
		}
		key := strings.ToLower(name)
		result = append(result, FunctionOccurrence{Name: name, Args: args, Occurrence: counts[key], SourceSpan: span})
		counts[key]++
	}
	return result, nil
}

func (s *Service) editFunction(source string, operation OperationType, mutation *FunctionMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.Name)) {
		return "", fmt.Errorf("function name is required and must be an identifier")
	}
	if operation != OperationRemoveFunction && strings.EqualFold(mutation.Name, "use_connector") {
		if len(mutation.Args) != 2 {
			return "", fmt.Errorf("use_connector requires view and connector arguments")
		}
		if err := s.validateConnector(sqltext.TrimQuote(mutation.Args[1])); err != nil {
			return "", err
		}
	}
	if operation != OperationRemoveFunction && strings.EqualFold(mutation.Name, "use_cache") {
		if len(mutation.Args) != 2 {
			return "", fmt.Errorf("use_cache requires view and cache arguments")
		}
		if err := s.validateCache(source, sqltext.TrimQuote(mutation.Args[1])); err != nil {
			return "", err
		}
	}
	rendered, err := renderFunction(mutation.Name, mutation.Args)
	if err != nil {
		return "", err
	}
	projection, err := outerProjection(source)
	if err != nil {
		return "", err
	}
	if operation == OperationAddFunction {
		return dql.ApplyPatch(source, dql.SourceSpan{Start: projection.end, End: projection.end}, ", "+rendered+" ")
	}
	if mutation.ExpectedArgs == nil {
		return "", fmt.Errorf("%s requires expectedArgs to verify the current call", operation)
	}
	functions, err := inspectFunctions(source)
	if err != nil {
		return "", err
	}
	var match *FunctionOccurrence
	for index := range functions {
		candidate := &functions[index]
		if strings.EqualFold(candidate.Name, mutation.Name) && candidate.Occurrence == mutation.Occurrence {
			match = candidate
			break
		}
	}
	if match == nil {
		return "", fmt.Errorf("function %s occurrence %d was not found in the outer projection", mutation.Name, mutation.Occurrence)
	}
	if !equalArguments(match.Args, mutation.ExpectedArgs) {
		return "", fmt.Errorf("function %s occurrence %d arguments changed: got %v", mutation.Name, mutation.Occurrence, match.Args)
	}
	span := match.SourceSpan
	if operation == OperationRemoveFunction {
		span = removalSpan(source, projection, span)
		rendered = ""
	}
	return dql.ApplyPatch(source, span, rendered)
}

func equalArguments(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if strings.TrimSpace(sqltext.TrimQuote(actual[i])) != strings.TrimSpace(sqltext.TrimQuote(expected[i])) {
			return false
		}
	}
	return true
}

func (s *Service) editSetting(source string, mutation *SettingMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.Name)) {
		return "", fmt.Errorf("setting name is required and must be an identifier")
	}
	if strings.EqualFold(mutation.Name, "connector") && !mutation.Remove {
		if len(mutation.Args) != 1 {
			return "", fmt.Errorf("connector setting requires one connector name")
		}
		if err := s.validateConnector(sqltext.TrimQuote(mutation.Args[0])); err != nil {
			return "", err
		}
	}
	enableCube := !mutation.Remove && (strings.EqualFold(mutation.Name, "cube") || strings.EqualFold(mutation.Name, "report"))
	enableCompose := !mutation.Remove && strings.EqualFold(mutation.Name, "cubeCompose") && firstBoolean(mutation.Args)
	if enableCube || enableCompose {
		if err := validateSimpleGroupedMain(source); err != nil {
			return "", err
		}
	}
	if enableCompose {
		prepared := dql.PrepareSource(source)
		if prepared.Directives == nil || prepared.Directives.Settings == nil || prepared.Directives.Settings.Report == nil || !prepared.Directives.Settings.Report.Enabled {
			return "", fmt.Errorf("cubeCompose activation requires cube or report to be enabled in the submitted DQL")
		}
	}
	args := mutation.Args
	if mutation.Remove {
		args = nil
	}
	tail, err := renderSettingOptions(mutation.Options)
	if err != nil {
		return "", err
	}
	return dql.UpsertSettingTail(source, mutation.Name, args, tail)
}

func renderSettingOptions(options []FunctionMutation) (string, error) {
	var result strings.Builder
	for _, option := range options {
		if !validIdentifier(strings.TrimSpace(option.Name)) {
			return "", fmt.Errorf("setting option name is required and must be an identifier")
		}
		for _, arg := range option.Args {
			if strings.TrimSpace(arg) == "" {
				return "", fmt.Errorf("setting option %s has an empty argument", option.Name)
			}
		}
		result.WriteByte('.')
		result.WriteString(strings.TrimSpace(option.Name))
		result.WriteByte('(')
		result.WriteString(strings.Join(option.Args, ", "))
		result.WriteByte(')')
	}
	return result.String(), nil
}

func firstBoolean(args []string) bool {
	if len(args) == 0 {
		return false
	}
	value, err := strconv.ParseBool(strings.TrimSpace(sqltext.TrimQuote(args[0])))
	return err == nil && value
}

func (s *Service) validateConnector(name string) error {
	for _, available := range s.config.AvailableConnectors {
		if strings.EqualFold(strings.TrimSpace(available), strings.TrimSpace(name)) {
			return nil
		}
	}
	return fmt.Errorf("connector %q is not available to the reader builder service", name)
}

func (s *Service) validateCache(source, name string) error {
	if slicesContainsFold(s.config.AvailableCaches, name) {
		return nil
	}
	prepared := dql.PrepareSource(source)
	if prepared.Directives != nil && prepared.Directives.Settings != nil {
		cache := prepared.Directives.Settings.Cache
		if cache != nil && cache.Enabled && strings.EqualFold(strings.TrimSpace(cache.Name), strings.TrimSpace(name)) {
			return nil
		}
	}
	return fmt.Errorf("cache %q has no enabled configuration available to the reader builder service", name)
}

func renderFunction(name string, args []string) (string, error) {
	name = strings.TrimSpace(name)
	if !validIdentifier(name) {
		return "", fmt.Errorf("invalid function name %q", name)
	}
	for _, arg := range args {
		if strings.TrimSpace(arg) == "" {
			return "", fmt.Errorf("function %s has an empty argument", name)
		}
	}
	if strings.EqualFold(name, "cast") {
		if len(args) != 2 {
			return "", fmt.Errorf("cast requires value and type arguments")
		}
		return name + "(" + strings.TrimSpace(args[0]) + " AS " + strings.Trim(strings.TrimSpace(args[1]), "'\"") + ")", nil
	}
	return name + "(" + strings.Join(args, ", ") + ")", nil
}

func outerProjection(source string) (*projectionSource, error) {
	prepared := dql.PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	if len(prepared.Statements) != 1 || prepared.Statements[0] == nil {
		return nil, fmt.Errorf("reader builder requires exactly one DQL statement")
	}
	statement := prepared.Statements[0]
	sqlSource := prepared.SQL[statement.SQLStart:statement.SQLEnd]
	selectAt := sqltext.FindTopLevelKeyword(sqlSource, "select", 0)
	fromAt := sqltext.FindTopLevelKeyword(sqlSource, "from", selectAt+len("select"))
	if selectAt < 0 || fromAt < 0 {
		return nil, fmt.Errorf("outer SELECT projection was not found")
	}
	base := prepared.TrimPrefix + statement.SQLStart
	projectionStart := base + selectAt + len("select")
	projectionEnd := base + fromAt
	return &projectionSource{start: projectionStart, end: projectionEnd, items: splitProjectionItems(source, projectionStart, projectionEnd)}, nil
}

func splitProjectionItems(source string, start, end int) []dql.SourceSpan {
	var result []dql.SourceSpan
	itemStart, depth := start, 0
	scanner := sqltext.NewCodeScanner(source, start)
	for i, more := scanner.Next(); more && i < end; i, more = scanner.Next() {
		ch := source[i]
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				result = appendTrimmedSpan(result, source, itemStart, i)
				itemStart = i + 1
			}
		}
	}
	return appendTrimmedSpan(result, source, itemStart, end)
}

func appendTrimmedSpan(spans []dql.SourceSpan, source string, start, end int) []dql.SourceSpan {
	for start < end && sqltext.IsWhitespace(source[start]) {
		start++
	}
	for end > start && sqltext.IsWhitespace(source[end-1]) {
		end--
	}
	if start < end {
		spans = append(spans, dql.SourceSpan{Start: start, End: end})
	}
	return spans
}

func removalSpan(source string, projection *projectionSource, target dql.SourceSpan) dql.SourceSpan {
	start, end := target.Start, target.End
	for start > projection.start && sqltext.IsWhitespace(source[start-1]) {
		start--
	}
	if start > projection.start && source[start-1] == ',' {
		return dql.SourceSpan{Start: start - 1, End: end}
	}
	for end < projection.end && sqltext.IsWhitespace(source[end]) {
		end++
	}
	if end < projection.end && source[end] == ',' {
		end++
	}
	return dql.SourceSpan{Start: start, End: end}
}

func validIdentifier(value string) bool {
	if value == "" || !(value[0] == '_' || value[0] >= 'a' && value[0] <= 'z' || value[0] >= 'A' && value[0] <= 'Z') {
		return false
	}
	for i := 1; i < len(value); i++ {
		ch := value[i]
		if ch != '_' && !(ch >= 'a' && ch <= 'z') && !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}
