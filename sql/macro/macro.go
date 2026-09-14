package macro

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx/metadata/info"
)

const (
	nonWindowSQLToken = "$View.NonWindowSQL"
	viewCallPrefix    = "$View."
)

type ParentKeyCall struct {
	Raw     string
	Method  string
	Prefix  string
	Columns []string
	Start   int
	End     int
}

type NonWindowSQLAccess struct {
	Raw   string
	Alias string
	Start int
	End   int
}

type nonWindowSQLReference struct {
	NonWindowSQLAccess
	protected bool
}

var parentKeyMethods = map[string]bool{
	"ParentJoinOn":          true,
	"ParentCompositeJoinOn": true,
	"AndParentJoinOn":       true,
	"ColIn":                 true,
}

// ParentKeyCalls parses the original Datly $View parent-key helper family in
// source order. It uses balanced-group parsing and an explicit method grammar.
func ParentKeyCalls(sql string) ([]ParentKeyCall, error) {
	var result []ParentKeyCall
	for searchFrom := 0; searchFrom < len(sql); {
		start := sqltext.FindCodeToken(sql, viewCallPrefix, searchFrom)
		if start == -1 {
			break
		}
		nameStart := start + len(viewCallPrefix)
		nameEnd := nameStart
		for nameEnd < len(sql) && ((sql[nameEnd] >= 'A' && sql[nameEnd] <= 'Z') || (sql[nameEnd] >= 'a' && sql[nameEnd] <= 'z')) {
			nameEnd++
		}
		method := sql[nameStart:nameEnd]
		if !parentKeyMethods[method] {
			searchFrom = nameEnd
			continue
		}
		cursor := nameEnd
		for cursor < len(sql) && sqltext.IsWhitespace(sql[cursor]) {
			cursor++
		}
		group, end, ok := sqltext.ReadGroupString(sql, cursor, '(', ')')
		if !ok {
			return nil, fmt.Errorf("invalid $View.%s call", method)
		}
		args := sqltext.TrimQuotedArgs(sqltext.SplitArgs(group[1 : len(group)-1]))
		call, err := newParentKeyCall(sql[start:end], method, args)
		if err != nil {
			return nil, err
		}
		call.Start = start
		call.End = end
		result = append(result, call)
		searchFrom = end
	}
	return result, nil
}

func newParentKeyCall(raw, method string, args []string) (ParentKeyCall, error) {
	call := ParentKeyCall{Raw: raw, Method: method}
	switch method {
	case "ParentJoinOn":
		call.Prefix = "AND"
		call.Columns = args
		if len(args) > 1 {
			call.Prefix = args[0]
			call.Columns = args[1:]
		}
	case "ParentCompositeJoinOn":
		if len(args) > 0 {
			call.Prefix = args[0]
			call.Columns = args[1:]
		}
	case "AndParentJoinOn":
		call.Prefix = "AND"
		call.Columns = args
	case "ColIn":
		if len(args) > 0 {
			call.Prefix = args[0]
		}
		if len(args) > 1 {
			call.Columns = args[1:2]
		}
	}
	if err := validateParentKeyPrefix(call.Prefix); err != nil {
		return ParentKeyCall{}, err
	}
	for _, column := range call.Columns {
		if err := validateColumn(column); err != nil {
			return ParentKeyCall{}, fmt.Errorf("invalid $View.%s column: %w", method, err)
		}
	}
	return call, nil
}

func validateParentKeyPrefix(prefix string) error {
	switch strings.ToUpper(strings.TrimSpace(prefix)) {
	case "", "WHERE", "AND", "OR":
		return nil
	default:
		return fmt.Errorf("invalid parent-key prefix %q", prefix)
	}
}

func validateColumn(column string) error {
	parsed, err := sqlparser.ParseQuery("SELECT " + column + " FROM parent_key_source")
	if err != nil || parsed == nil || len(parsed.List) != 1 || parsed.List[0] == nil {
		if err == nil {
			err = fmt.Errorf("column is required")
		}
		return err
	}
	item := parsed.List[0]
	if item.Alias != "" || normalizeSQLFragment(sqlparser.Stringify(item)) != normalizeSQLFragment(column) {
		return fmt.Errorf("%q is not a column reference", column)
	}
	switch item.Expr.(type) {
	case *expr.Ident, *expr.Selector:
		return nil
	default:
		return fmt.Errorf("%q is not a column reference", column)
	}
}

func normalizeSQLFragment(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func ExpandParentKeyCalls(sql string, dialect *info.Dialect, scalarValues []any, compositeRows [][]interface{}) (string, []any, int, error) {
	calls, err := ParentKeyCalls(sql)
	if err != nil || len(calls) == 0 {
		return sql, nil, 0, err
	}
	var args []any
	expander := ParentKeyExpander{Dialect: dialect, ScalarValues: scalarValues, CompositeRows: compositeRows}
	var builder strings.Builder
	last := 0
	for _, call := range calls {
		fragment, callArgs, err := expander.Expand(call)
		if err != nil {
			return "", nil, 0, err
		}
		builder.WriteString(sql[last:call.Start])
		builder.WriteString(fragment)
		last = call.End
		args = append(args, callArgs...)
	}
	builder.WriteString(sql[last:])
	result := builder.String()
	return result, args, len(calls), nil
}

func StripParentKeyCalls(sql string) (string, int, error) {
	calls, err := ParentKeyCalls(sql)
	if err != nil || len(calls) == 0 {
		return sql, 0, err
	}
	var builder strings.Builder
	last := 0
	for _, call := range calls {
		builder.WriteString(sql[last:call.Start])
		last = call.End
	}
	builder.WriteString(sql[last:])
	return builder.String(), len(calls), nil
}

func parentKeyFragment(call ParentKeyCall, dialect *info.Dialect, scalarValues []any, compositeRows [][]interface{}) (string, []any) {
	prefix := strings.ToUpper(strings.TrimSpace(call.Prefix))
	if prefix != "" {
		prefix += " "
	}
	if len(call.Columns) > 1 || call.Method == "ParentCompositeJoinOn" {
		if len(call.Columns) == 0 || len(compositeRows) == 0 {
			return prefix + "1 = 0", nil
		}
		return prefix + dialect.CompositeIn(call.Columns, len(compositeRows)), flattenRows(compositeRows)
	}
	if len(call.Columns) == 0 || len(scalarValues) == 0 {
		return prefix + "1 = 0", nil
	}
	return prefix + dialect.CompositeIn(call.Columns, len(scalarValues)), append([]any(nil), scalarValues...)
}

func flattenRows(rows [][]interface{}) []any {
	var result []any
	for _, row := range rows {
		result = append(result, row...)
	}
	return result
}

// ExpandNonWindowSQL replaces $View.NonWindowSQL and any explicitly named
// $View.<ParentViewName>.NonWindowSQL aliases with the supplied non-window SQL,
// returning the rewritten text and the number of replaced occurrences.
func ExpandNonWindowSQL(sql string, nonWindowSQL string, aliases ...string) (string, int) {
	if strings.TrimSpace(sql) == "" {
		return sql, 0
	}
	allowed := nonWindowAliasSet(aliases)
	var replacements []sourceReplacement
	for _, access := range NonWindowSQLAccesses(sql) {
		if access.Alias != "" && !allowed[access.Alias] {
			continue
		}
		replacements = append(replacements, sourceReplacement{start: access.Start, end: access.End, value: nonWindowSQL})
	}
	return applySourceReplacements(sql, replacements), len(replacements)
}

// NonWindowSQLAccesses parses unqualified and named non-window SQL references
// at SQL code positions. Protected SQL text is skipped by the shared lexer.
func NonWindowSQLAccesses(sql string) []NonWindowSQLAccess {
	var result []NonWindowSQLAccess
	for _, reference := range nonWindowSQLReferences(sql) {
		if reference.protected {
			continue
		}
		result = append(result, reference.NonWindowSQLAccess)
	}
	return result
}

func nonWindowSQLReferences(sql string) []nonWindowSQLReference {
	var result []nonWindowSQLReference
	for offset := 0; offset < len(sql); {
		relative := strings.Index(sql[offset:], viewCallPrefix)
		if relative == -1 {
			break
		}
		start := offset + relative
		access, ok := nonWindowSQLAccessAt(sql, start)
		if !ok {
			offset = start + len(viewCallPrefix)
			continue
		}
		result = append(result, nonWindowSQLReference{
			NonWindowSQLAccess: access,
			protected:          sqltext.ProtectionAt(sql, start) != "",
		})
		offset = access.End
	}
	return result
}

func ValidateNonWindowSQLAliases(sql string, aliases ...string) error {
	allowed := nonWindowAliasSet(aliases)
	for _, access := range NonWindowSQLAccesses(sql) {
		if access.Alias == "" || allowed[access.Alias] {
			continue
		}
		return fmt.Errorf("unknown parent view alias %q in %s", access.Alias, access.Raw)
	}
	return nil
}

func nonWindowSQLAccessAt(source string, start int) (NonWindowSQLAccess, bool) {
	cursor := start + len(viewCallPrefix)
	nameStart := cursor
	for cursor < len(source) && isMacroIdentifierPart(source[cursor]) {
		cursor++
	}
	if nameStart == cursor {
		return NonWindowSQLAccess{}, false
	}
	first := source[nameStart:cursor]
	alias := ""
	if first != "NonWindowSQL" {
		alias = first
		if cursor >= len(source) || source[cursor] != '.' {
			return NonWindowSQLAccess{}, false
		}
		cursor++
		const property = "NonWindowSQL"
		if !strings.HasPrefix(source[cursor:], property) {
			return NonWindowSQLAccess{}, false
		}
		cursor += len(property)
	}
	if !nonWindowTokenBoundary(source, cursor) {
		return NonWindowSQLAccess{}, false
	}
	return NonWindowSQLAccess{
		Raw:   source[start:cursor],
		Alias: alias,
		Start: start,
		End:   cursor,
	}, true
}

func nonWindowAliasSet(aliases []string) map[string]bool {
	result := make(map[string]bool, len(aliases))
	for _, alias := range aliases {
		if alias = strings.TrimSpace(alias); alias != "" {
			result[alias] = true
		}
	}
	return result
}

// PrepareNonWindowSQLTemplate converts authored property-style non-window SQL
// access into the typed View context method used by compiled SQL templates.
// Matching tokens in protected SQL text are wrapped in Velty's literal escape
// syntax so evaluation emits them unchanged.
func PrepareNonWindowSQLTemplate(sql string, aliases ...string) (string, int) {
	if strings.TrimSpace(sql) == "" {
		return sql, 0
	}
	var replacements []sourceReplacement
	rewritten := 0
	allowed := nonWindowAliasSet(aliases)
	for _, reference := range nonWindowSQLReferences(sql) {
		access := reference.NonWindowSQLAccess
		if reference.protected {
			replacements = append(replacements, sourceReplacement{
				start: access.Start,
				end:   access.End,
				value: "#[[" + access.Raw + "]]#",
			})
			continue
		}
		if access.Alias != "" && !allowed[access.Alias] {
			continue
		}
		replacement := "$View.NonWindowSQL()"
		if nextNonSpace(sql, access.End) == '(' {
			replacement = "$View.NonWindowSQL"
		}
		replacements = append(replacements, sourceReplacement{start: access.Start, end: access.End, value: replacement})
		rewritten++
	}
	return applySourceReplacements(sql, replacements), rewritten
}

func nonWindowTokenBoundary(source string, end int) bool {
	if end >= len(source) {
		return true
	}
	next := source[end]
	return next != '.' && !isMacroIdentifierPart(next)
}

func nextNonSpace(source string, offset int) byte {
	for offset < len(source) {
		switch source[offset] {
		case ' ', '\t', '\r', '\n':
			offset++
		default:
			return source[offset]
		}
	}
	return 0
}

func isMacroIdentifierPart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
}

type sourceReplacement struct {
	start int
	end   int
	value string
}

func applySourceReplacements(source string, replacements []sourceReplacement) string {
	if len(replacements) == 0 {
		return source
	}
	sort.Slice(replacements, func(i, j int) bool {
		return replacements[i].start > replacements[j].start
	})
	result := source
	for _, replacement := range replacements {
		result = result[:replacement.start] + replacement.value + result[replacement.end:]
	}
	return result
}

func NonWindowSQLTokens(aliases ...string) []string {
	return nonWindowSQLTokens(aliases...)
}

func nonWindowSQLTokens(aliases ...string) []string {
	result := []string{nonWindowSQLToken}
	seen := map[string]bool{nonWindowSQLToken: true}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		token := "$View." + alias + ".NonWindowSQL"
		if seen[token] {
			continue
		}
		seen[token] = true
		result = append(result, token)
	}
	return result
}

func normalizeColumn(input string) string {
	input = strings.TrimSpace(input)
	if idx := strings.LastIndex(input, "."); idx != -1 {
		input = input[idx+1:]
	}
	return strings.ToLower(strings.TrimSpace(input))
}
