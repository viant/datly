package pipeline

import (
	"reflect"
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser/query"
)

func BuildRead(sourceName, sqlText string) (*plan.View, []*dqlshape.Diagnostic, error) {
	parserSQL := normalizeParserSQL(sqlText)
	queryNode, parseDiag, err := ParseSelectWithDiagnostic(parserSQL)
	if err != nil && parserSQL != sqlText {
		if rawNode, _, rawErr := ParseSelectWithDiagnostic(sqlText); rawErr == nil && isUsableQuery(rawNode) {
			queryNode = rawNode
			parserSQL = sqlText
			parseDiag = nil
			err = nil
		}
	}
	if err == nil && needsFallbackParse(sqlText, queryNode) {
		fallbackSQL := normalizeParserSQL(sqlText)
		if fallbackNode, _, fallbackErr := ParseSelectWithDiagnostic(fallbackSQL); fallbackErr == nil && isUsableQuery(fallbackNode) {
			queryNode = fallbackNode
			parserSQL = fallbackSQL
			parseDiag = nil
			err = nil
		}
	}
	if hasTemplateSignals(sqlText) && (err != nil || parseDiag != nil) {
		if parseDiag != nil {
			parseDiag.Severity = dqlshape.SeverityWarning
		}
		var diags []*dqlshape.Diagnostic
		if parseDiag != nil {
			diags = append(diags, parseDiag)
		}
		return buildLooseRead(sourceName, sqlText), diags, nil
	}
	var diags []*dqlshape.Diagnostic
	if parseDiag != nil {
		diags = append(diags, parseDiag)
	}
	if err != nil {
		if hasTemplateSignals(sqlText) {
			if parseDiag != nil {
				parseDiag.Severity = dqlshape.SeverityWarning
			}
			return buildLooseRead(sourceName, sqlText), diags, nil
		}
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
	view := &plan.View{
		Path:        name,
		Holder:      name,
		Name:        name,
		Mode:        "SQLQuery",
		Table:       table,
		SQL:         sqlText,
		Cardinality: cardinality,
		FieldType:   fieldType,
		ElementType: elementType,
		Relations:   relations,
	}
	return view, diags, nil
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

func normalizeParserSQL(sqlText string) string {
	if sqlText == "" {
		return sqlText
	}
	return replaceTemplateTokens(sqlText)
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

func replaceTemplateTokens(input string) string {
	var b strings.Builder
	b.Grow(len(input))
	for i := 0; i < len(input); {
		if input[i] != '$' {
			b.WriteByte(input[i])
			i++
			continue
		}
		if i+1 < len(input) && input[i+1] == '{' {
			body, end, ok := readReadTemplateExpr(input, i+1)
			if !ok {
				b.WriteByte(input[i])
				i++
				continue
			}
			replacement, keep := normalizeTemplateExprBody(body)
			if keep {
				b.WriteString(input[i : end+1])
			} else {
				b.WriteString(replacement)
			}
			i = end + 1
			continue
		}
		token, end, ok := readReadSelector(input, i)
		if !ok {
			b.WriteByte(input[i])
			i++
			continue
		}
		if strings.EqualFold(token, "$criteria.AppendBinding") {
			pos := skipReadSpaces(input, end)
			if pos < len(input) && input[pos] == '(' {
				_, close, ok := readReadCallBody(input, pos)
				if ok {
					b.WriteByte('1')
					i = close + 1
					continue
				}
			}
		}
		if isReadReservedToken(token) {
			b.WriteString(token)
		} else {
			b.WriteByte('1')
		}
		i = end
	}
	return b.String()
}

func normalizeTemplateExprBody(body string) (string, bool) {
	trimmed := strings.TrimSpace(body)
	if isReadReservedName(trimmed) {
		return "", true
	}
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, `build("where")`) || strings.Contains(lower, "build('where')") {
		return " WHERE 1 ", false
	}
	if strings.Contains(lower, `build("and")`) || strings.Contains(lower, "build('and')") {
		return " AND 1 ", false
	}
	return "1", false
}

func readReadTemplateExpr(input string, openBrace int) (string, int, bool) {
	if openBrace <= 0 || openBrace >= len(input) || input[openBrace] != '{' || input[openBrace-1] != '$' {
		return "", -1, false
	}
	for i := openBrace + 1; i < len(input); i++ {
		if input[i] == '}' {
			return input[openBrace+1 : i], i, true
		}
	}
	return "", -1, false
}

func readReadSelector(input string, start int) (string, int, bool) {
	if start < 0 || start >= len(input) || input[start] != '$' {
		return "", start, false
	}
	i := start + 1
	if i >= len(input) || !isReadIdentifierStart(input[i]) {
		return "", start, false
	}
	i++
	for i < len(input) && isReadIdentifierPart(input[i]) {
		i++
	}
	for i < len(input) && input[i] == '.' {
		i++
		if i >= len(input) || !isReadIdentifierStart(input[i]) {
			return "", start, false
		}
		i++
		for i < len(input) && isReadIdentifierPart(input[i]) {
			i++
		}
	}
	return input[start:i], i, true
}

func readReadCallBody(input string, openParen int) (string, int, bool) {
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

func isReadReservedToken(token string) bool {
	if len(token) > 0 && token[0] == '$' {
		token = token[1:]
	}
	return isReadReservedName(token)
}

func isReadReservedName(name string) bool {
	return name == "sql.Insert" || name == "sql.Update" || name == "Nop"
}

func skipReadSpaces(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t', '\n', '\r':
			index++
		default:
			return index
		}
	}
	return index
}

func isReadIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isReadIdentifierPart(ch byte) bool {
	return isReadIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}
