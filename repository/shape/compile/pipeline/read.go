package pipeline

// read.go — SELECT compilation: parses DQL into a plan.View using
// multi-strategy parse with template-signal fallback.
// SQL normalization and token utilities live in read_normalize.go.

import (
	"reflect"
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

// BuildRead compiles a SELECT DQL fragment into a plan.View.
// It applies multiple parse strategies and gracefully degrades to a
// loose (schema-less) view for template-driven SQL that cannot be fully parsed.
func BuildRead(sourceName, sqlText string) (*plan.View, []*dqlshape.Diagnostic, error) {
	return BuildReadWithConsts(sourceName, sqlText, nil)
}

func BuildReadWithConsts(sourceName, sqlText string, consts map[string]string) (*plan.View, []*dqlshape.Diagnostic, error) {
	queryNode, parseDiag, parserSQL, err := resolveQueryNode(sqlText)

	// Template-driven SQL may legitimately fail strict parsing; treat as warning.
	if (err != nil || parseDiag != nil) && hasTemplateSignals(sqlText) {
		if parseDiag != nil {
			parseDiag.Severity = dqlshape.SeverityWarning
		}
		view := buildLooseRead(sourceName, sqlText)
		applyConstTables(view, consts)
		return view, collectDiags(parseDiag), nil
	}

	var diags []*dqlshape.Diagnostic
	if parseDiag != nil {
		diags = append(diags, parseDiag)
	}
	if err != nil {
		return nil, diags, nil
	}

	relations, relationDiags := ExtractJoinRelations(parserSQL, queryNode)
	diags = append(diags, relationDiags...)

	name, table, inferErr := InferRoot(queryNode, sourceName)
	if inferErr != nil {
		return nil, nil, inferErr
	}
	fallback := SanitizeName(sourceName)
	if name == fallback && table == fallback {
		if derived := inferRootFromRelations(relations); derived != "" {
			name = derived
			table = derived
		}
	}

	fieldType, elementType, cardinality := InferProjectionType(queryNode)
	if fieldType == nil || elementType == nil {
		fieldType = reflect.TypeOf([]map[string]interface{}{})
		elementType = reflect.TypeOf(map[string]interface{}{})
		cardinality = "many"
	}
	rootSQL := sqlText
	if rawRoot := extractRootSQLFromRaw(sqlText); rawRoot != "" {
		rootSQL = rawRoot
	} else if queryNode.From.X != nil {
		fromExpr := strings.TrimSpace(sqlparser.Stringify(queryNode.From.X))
		fromExpr = trimJoinSuffix(fromExpr)
		candidate := extractParenthesizedSelect(fromExpr)
		if candidate == "" {
			candidate = unwrapReadParens(fromExpr)
		}
		if candidate != "" {
			rootSQL = candidate
		}
	}
	view := &plan.View{
		Path:        name,
		Holder:      name,
		Name:        name,
		Mode:        "SQLQuery",
		Table:       table,
		SQL:         rootSQL,
		Cardinality: cardinality,
		FieldType:   fieldType,
		ElementType: elementType,
		Relations:   relations,
	}
	exceptByAlias := extractExceptColumnsByNamespace(queryNode)
	if except := lookupExceptColumns(exceptByAlias, name); len(except) > 0 {
		view.Declaration = &plan.ViewDeclaration{ColumnsConfig: except}
	}
	applyRelationExceptColumns(relations, exceptByAlias)
	applyConstTables(view, consts)
	return view, diags, nil
}

func applyConstTables(view *plan.View, consts map[string]string) {
	if view == nil || len(consts) == 0 {
		return
	}
	view.Table = resolveConstTable(view.Table, consts)
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		relation.Table = resolveConstTable(relation.Table, consts)
	}
}

func resolveConstTable(table string, consts map[string]string) string {
	trimmed := strings.TrimSpace(strings.Trim(table, "`\""))
	if token := unsafeSelectorToken(trimmed); token != "" {
		if resolved := resolveConstValue(token, consts); resolved != "" {
			return resolved
		}
	}
	for key, value := range consts {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		placeholder := "Unsafe_" + key
		if strings.EqualFold(trimmed, placeholder) {
			return value
		}
		templatePlaceholder := "${Unsafe." + key + "}"
		if strings.EqualFold(trimmed, templatePlaceholder) {
			return value
		}
		selectorPlaceholder := "$Unsafe." + key
		if strings.EqualFold(trimmed, selectorPlaceholder) {
			return value
		}
		if strings.Contains(table, placeholder) {
			table = strings.ReplaceAll(table, placeholder, value)
		}
		if strings.Contains(table, templatePlaceholder) {
			table = strings.ReplaceAll(table, templatePlaceholder, value)
		}
		if strings.Contains(table, selectorPlaceholder) {
			table = strings.ReplaceAll(table, selectorPlaceholder, value)
		}
	}
	return table
}

func unsafeSelectorToken(input string) string {
	if strings.HasPrefix(input, "${Unsafe.") && strings.HasSuffix(input, "}") {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(input, "${Unsafe."), "}"))
	}
	if strings.HasPrefix(input, "$Unsafe.") {
		return strings.TrimSpace(strings.TrimPrefix(input, "$Unsafe."))
	}
	return ""
}

func resolveConstValue(token string, consts map[string]string) string {
	if token == "" || len(consts) == 0 {
		return ""
	}
	for key, value := range consts {
		if strings.EqualFold(strings.TrimSpace(key), token) && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// resolveQueryNode attempts to parse sqlText into a query AST using up to
// three strategies:
//  1. Parse the normalised form.
//  2. If normalisation broke the SQL, fall back to the raw form.
//  3. If the parsed result is structurally incomplete, retry with the
//     normalised form to pick up joins the raw parse missed.
//
// It returns the best node, any diagnostic, the effective SQL used, and any
// parse error.
func resolveQueryNode(sqlText string) (node *query.Select, diag *dqlshape.Diagnostic, effectiveSQL string, err error) {
	parserSQL := normalizeParserSQL(sqlText)
	node, diag, err = ParseSelectWithDiagnostic(parserSQL)

	// Strategy 2: normalisation may have broken the SQL; try raw form.
	if err != nil && parserSQL != sqlText {
		if rawNode, _, rawErr := ParseSelectWithDiagnostic(sqlText); rawErr == nil && isUsableQuery(rawNode) {
			return rawNode, nil, sqlText, nil
		}
	}

	// Strategy 3: parsed OK but result is incomplete (no FROM or missing JOINs);
	// retry with the normalised form.
	if err == nil && needsFallbackParse(sqlText, node) {
		fallbackSQL := normalizeParserSQL(sqlText)
		if fallbackNode, _, fallbackErr := ParseSelectWithDiagnostic(fallbackSQL); fallbackErr == nil && isUsableQuery(fallbackNode) {
			return fallbackNode, nil, fallbackSQL, nil
		}
	}
	return node, diag, parserSQL, err
}

func buildLooseRead(sourceName, sqlText string) *plan.View {
	name, table := inferLooseRoot(sourceName, sqlText)
	fieldType := reflect.TypeOf([]map[string]interface{}{})
	elementType := reflect.TypeOf(map[string]interface{}{})
	return &plan.View{
		Path:        name,
		Holder:      name,
		Name:        name,
		Mode:        "SQLQuery",
		Table:       table,
		SQL:         sqlText,
		Cardinality: "many",
		FieldType:   fieldType,
		ElementType: elementType,
	}
}

func inferLooseRoot(sourceName, sqlText string) (string, string) {
	name := SanitizeName(sourceName)
	if name == "" {
		name = "DQLView"
	}
	if table := extractSimpleFromTable(sqlText); table != "" {
		return name, table
	}
	return name, name
}

func hasTemplateSignals(sqlText string) bool {
	lower := strings.ToLower(sqlText)
	return strings.Contains(lower, "#if(") || strings.Contains(lower, "#elseif(") || strings.Contains(lower, "#else") ||
		strings.Contains(lower, "#end") || strings.Contains(lower, "${") || strings.Contains(lower, "$unsafe.") ||
		strings.Contains(lower, "$view.") || strings.Contains(lower, "$predicate.")
}

func isUsableQuery(queryNode *query.Select) bool {
	return queryNode != nil && queryNode.From.X != nil
}

func needsFallbackParse(rawSQL string, queryNode *query.Select) bool {
	if !isUsableQuery(queryNode) {
		return true
	}
	lower := strings.ToLower(rawSQL)
	if strings.Contains(lower, " join ") && len(queryNode.Joins) == 0 {
		return true
	}
	return false
}

func inferRootFromRelations(relations []*plan.Relation) string {
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			name := SanitizeName(link.ParentNamespace)
			if name != "" {
				return name
			}
		}
	}
	return ""
}

func extractRootExceptColumns(queryNode *query.Select, rootName string) map[string]*plan.ViewColumnConfig {
	return lookupExceptColumns(extractExceptColumnsByNamespace(queryNode), rootName)
}

func applyRelationExceptColumns(relations []*plan.Relation, exceptByAlias map[string]map[string]*plan.ViewColumnConfig) {
	if len(relations) == 0 || len(exceptByAlias) == 0 {
		return
	}
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		if columns := lookupExceptColumns(exceptByAlias, relation.Ref); len(columns) > 0 {
			relation.ColumnsConfig = columns
		}
	}
}

func lookupExceptColumns(exceptByAlias map[string]map[string]*plan.ViewColumnConfig, alias string) map[string]*plan.ViewColumnConfig {
	if len(exceptByAlias) == 0 {
		return nil
	}
	alias = strings.ToLower(strings.TrimSpace(alias))
	if alias == "" {
		return nil
	}
	result := exceptByAlias[alias]
	if len(result) == 0 {
		return nil
	}
	ret := make(map[string]*plan.ViewColumnConfig, len(result))
	for key, cfg := range result {
		ret[key] = cfg
	}
	return ret
}

func extractExceptColumnsByNamespace(queryNode *query.Select) map[string]map[string]*plan.ViewColumnConfig {
	if queryNode == nil {
		return nil
	}
	result := map[string]map[string]*plan.ViewColumnConfig{}
	for _, item := range queryNode.List {
		if item == nil || item.Expr == nil {
			continue
		}
		star, ok := item.Expr.(*expr.Star)
		if !ok || len(star.Except) == 0 {
			continue
		}
		selectorNs := ""
		if selector, ok := star.X.(*expr.Selector); ok {
			selectorNs = strings.ToLower(strings.TrimSpace(selector.Name))
		}
		if selectorNs == "" {
			continue
		}
		nsColumns := result[selectorNs]
		if nsColumns == nil {
			nsColumns = map[string]*plan.ViewColumnConfig{}
			result[selectorNs] = nsColumns
		}
		for _, exceptColumn := range star.Except {
			exceptColumn = strings.TrimSpace(exceptColumn)
			if exceptColumn == "" {
				continue
			}
			nsColumns[exceptColumn] = &plan.ViewColumnConfig{
				Tag: `internal:"true"`,
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func extractSimpleFromTable(sqlText string) string {
	lower := strings.ToLower(sqlText)
	for i := 0; i+4 <= len(lower); i++ {
		if lower[i] != 'f' || !strings.HasPrefix(lower[i:], "from") {
			continue
		}
		if i > 0 && isReadIdentifierPart(lower[i-1]) {
			continue
		}
		j := skipReadSpaces(sqlText, i+4)
		start := j
		if start >= len(sqlText) || !isReadIdentifierStart(sqlText[start]) {
			continue
		}
		j++
		for j < len(sqlText) && (isReadIdentifierPart(sqlText[j]) || sqlText[j] == '.' || sqlText[j] == '$') {
			j++
		}
		if start < j {
			return strings.Trim(sqlText[start:j], "`\"")
		}
	}
	return ""
}

func extractRootSQLFromRaw(sqlText string) string {
	if strings.TrimSpace(sqlText) == "" {
		return ""
	}
	lower := strings.ToLower(sqlText)
	depth := 0
	quote := byte(0)
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(sqlText) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || !hasReadWordAt(lower, i, "from") {
			continue
		}
		start := skipReadSpaces(sqlText, i+4)
		if start >= len(sqlText) {
			return ""
		}
		fromExpr := trimJoinSuffix(sqlText[start:])
		fromExpr = strings.TrimSpace(fromExpr)
		if fromExpr == "" {
			return ""
		}
		if candidate := extractParenthesizedSelect(fromExpr); candidate != "" {
			return candidate
		}
		fromExpr = unwrapReadParens(fromExpr)
		fromExpr = strings.TrimSpace(fromExpr)
		if fromExpr == "" {
			return ""
		}
		if strings.HasPrefix(strings.ToLower(fromExpr), "select ") {
			return fromExpr
		}
		return "SELECT * FROM " + fromExpr
	}
	return ""
}

func unwrapReadParens(input string) string {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '(' || input[len(input)-1] != ')' {
		return input
	}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(input)-1 {
				return input
			}
		}
	}
	if depth != 0 {
		return input
	}
	inner := strings.TrimSpace(input[1 : len(input)-1])
	if inner == "" {
		return input
	}
	return inner
}

func trimJoinSuffix(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || !isReadWordStart(input[i]) {
			continue
		}
		if hasReadWordAt(strings.ToLower(input), i, "join") {
			return strings.TrimSpace(input[:i])
		}
	}
	return input
}

func extractParenthesizedSelect(input string) string {
	input = strings.TrimSpace(input)
	if input == "" || input[0] != '(' {
		return ""
	}
	body, end, ok := readReadParenBody(input, 0)
	if !ok {
		return ""
	}
	tail := strings.TrimSpace(input[end+1:])
	if tail != "" && !isReadIdentifierStart(tail[0]) {
		return ""
	}
	body = strings.TrimSpace(body)
	if strings.HasPrefix(strings.ToLower(body), "select ") {
		return body
	}
	return ""
}

func readReadParenBody(input string, openParen int) (string, int, bool) {
	depth := 0
	quote := byte(0)
	for i := openParen; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return input[openParen+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func hasReadWordAt(lower string, pos int, word string) bool {
	if pos < 0 || pos+len(word) > len(lower) {
		return false
	}
	if lower[pos:pos+len(word)] != word {
		return false
	}
	if pos > 0 && isReadWordPart(lower[pos-1]) {
		return false
	}
	next := pos + len(word)
	if next < len(lower) && isReadWordPart(lower[next]) {
		return false
	}
	return true
}

func isReadWordStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isReadWordPart(ch byte) bool {
	return isReadWordStart(ch) || (ch >= '0' && ch <= '9')
}

// collectDiags returns a single-element slice for a non-nil diagnostic,
// or nil otherwise. Used to avoid repeated nil checks at call sites.
func collectDiags(diag *dqlshape.Diagnostic) []*dqlshape.Diagnostic {
	if diag == nil {
		return nil
	}
	return []*dqlshape.Diagnostic{diag}
}
