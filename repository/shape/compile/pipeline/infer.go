package pipeline

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

func InferRoot(queryNode *query.Select, fallback string) (string, string, error) {
	name := SanitizeName(fallback)
	if name == "" {
		name = "DQLView"
	}
	if queryNode == nil {
		return name, name, nil
	}
	if alias := SanitizeName(queryNode.From.Alias); alias != "" {
		name = alias
	}
	table := ""
	if queryNode.From.X != nil {
		table = strings.TrimSpace(sqlparser.Stringify(queryNode.From.X))
	}
	if name == "" || name == SanitizeName(fallback) {
		if subAlias := inferSubqueryAlias(table); subAlias != "" {
			name = subAlias
		}
	}
	if table == "" || strings.HasPrefix(table, "(") {
		if inferred := inferSubqueryTable(table); inferred != "" {
			table = inferred
		} else {
			table = name
		}
	}
	if name == "" {
		return "", "", fmt.Errorf("shape compile: failed to infer view name")
	}
	return name, table, nil
}

func inferSubqueryAlias(fromExpr string) string {
	fromExpr = strings.TrimSpace(fromExpr)
	if fromExpr == "" || !strings.HasPrefix(fromExpr, "(") {
		return ""
	}
	depth := 0
	closeIdx := -1
	for i := 0; i < len(fromExpr); i++ {
		switch fromExpr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeIdx = i
				i = len(fromExpr)
			}
		}
	}
	if closeIdx == -1 || closeIdx+1 >= len(fromExpr) {
		return ""
	}
	rest := strings.TrimSpace(fromExpr[closeIdx+1:])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "as ") {
		rest = strings.TrimSpace(rest[3:])
	}
	if rest == "" {
		return ""
	}
	end := 0
	for end < len(rest) {
		c := rest[end]
		if !(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (end > 0 && c >= '0' && c <= '9')) {
			break
		}
		end++
	}
	if end == 0 {
		return ""
	}
	return SanitizeName(rest[:end])
}

func inferSubqueryTable(fromExpr string) string {
	inner, ok := extractSubqueryBody(fromExpr)
	if !ok {
		return ""
	}
	normalized := normalizeParserSQL(inner)
	queryNode, _, err := ParseSelectWithDiagnostic(normalized)
	if err != nil || queryNode == nil {
		return ""
	}
	_, table, err := InferRoot(queryNode, "")
	if err != nil {
		return ""
	}
	table = strings.TrimSpace(strings.Trim(table, "`\""))
	if strings.EqualFold(table, "DQLView") {
		return ""
	}
	return table
}

func extractSubqueryBody(fromExpr string) (string, bool) {
	fromExpr = strings.TrimSpace(fromExpr)
	if !strings.HasPrefix(fromExpr, "(") {
		return "", false
	}
	depth := 0
	for i := 0; i < len(fromExpr); i++ {
		switch fromExpr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if i <= 1 {
					return "", false
				}
				return strings.TrimSpace(fromExpr[1:i]), true
			}
		}
	}
	return "", false
}

func InferProjectionType(queryNode *query.Select) (reflect.Type, reflect.Type, string) {
	if queryNode == nil || len(queryNode.List) == 0 || queryNode.List.IsStarExpr() {
		return reflect.TypeOf([]map[string]interface{}{}), reflect.TypeOf(map[string]interface{}{}), "many"
	}
	for _, item := range queryNode.List {
		if requiresDeferredProjectionType(sqlparser.Stringify(item)) {
			return reflect.TypeOf([]map[string]interface{}{}), reflect.TypeOf(map[string]interface{}{}), "many"
		}
	}
	fields := make([]reflect.StructField, 0, len(queryNode.List))
	used := map[string]int{}
	for index, item := range queryNode.List {
		column := sqlparser.NewColumn(item)
		columnName := strings.TrimSpace(column.Identity())
		if columnName == "" {
			columnName = fmt.Sprintf("col_%d", index+1)
		}
		fieldName := ExportedName(columnName)
		if fieldName == "" {
			fieldName = fmt.Sprintf("Col%d", index+1)
		}
		if count := used[fieldName]; count > 0 {
			fieldName = fmt.Sprintf("%s%d", fieldName, count+1)
		}
		used[fieldName]++

		typ := inferColumnType(sqlparser.Stringify(item), column.Type)
		veltyNames := []string{columnName}
		if fieldName != "" && fieldName != columnName {
			veltyNames = append(veltyNames, fieldName)
		}
		fields = append(fields, reflect.StructField{
			Name: fieldName,
			Type: typ,
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s,omitempty" sqlx:"name=%s" velty:"names=%s"`, lowerCamel(fieldName), columnName, strings.Join(veltyNames, "|"))),
		})
	}
	element := reflect.StructOf(fields)
	return reflect.SliceOf(element), element, "many"
}

func requiresDeferredProjectionType(expression string) bool {
	expression = strings.ToLower(strings.TrimSpace(expression))
	if expression == "" {
		return false
	}
	if strings.Contains(expression, ".*") {
		return true
	}
	if strings.Contains(expression, " except ") {
		return true
	}
	if strings.HasPrefix(expression, "allow_nulls(") {
		return true
	}
	return false
}

func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	runes := []rune(value)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func SanitizeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if value == strings.ToUpper(value) {
		value = strings.ToLower(value)
	}
	value = replaceNonWordWithUnderscore(value)
	value = strings.Trim(value, "_")
	if value == "" {
		return ""
	}
	if value[0] >= '0' && value[0] <= '9' {
		value = "V_" + value
	}
	return value
}

func ExportedName(value string) string {
	value = strings.TrimSpace(value)
	if preserved := preserveMixedCaseIdentifier(value); preserved != "" {
		return preserved
	}
	value = replaceNonWordWithUnderscore(value)
	value = strings.Trim(value, "_")
	if value == "" {
		return ""
	}
	parts := strings.Split(strings.ToLower(value), "_")
	for i, item := range parts {
		if item == "" {
			continue
		}
		parts[i] = strings.ToUpper(item[:1]) + item[1:]
	}
	name := strings.Join(parts, "")
	if name == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "N" + name
	}
	return name
}

func preserveMixedCaseIdentifier(value string) string {
	if value == "" {
		return ""
	}
	hasLower := false
	hasUpperAfterFirst := false
	for i, r := range value {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r)) {
			return ""
		}
		if unicode.IsLower(r) {
			hasLower = true
		}
		if i > 0 && unicode.IsUpper(r) {
			hasUpperAfterFirst = true
		}
	}
	if !hasLower || !hasUpperAfterFirst {
		return ""
	}
	runes := []rune(value)
	if len(runes) == 0 {
		return ""
	}
	if unicode.IsDigit(runes[0]) {
		return "N" + value
	}
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func replaceNonWordWithUnderscore(value string) string {
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	lastUnderscore := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		isWord := ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
		if isWord {
			b.WriteByte(ch)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return b.String()
}

func parseColumnType(dataType string) reflect.Type {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "", "string", "text", "varchar", "char", "uuid", "json", "jsonb":
		return reflect.TypeOf("")
	case "bool", "boolean":
		return reflect.TypeOf(false)
	case "int", "int32", "smallint", "integer", "signed":
		return reflect.TypeOf(int(0))
	case "int64", "bigint":
		return reflect.TypeOf(int64(0))
	case "float", "float32", "real":
		return reflect.TypeOf(float32(0))
	case "float64", "double", "numeric", "decimal":
		return reflect.TypeOf(float64(0))
	default:
		return reflect.TypeOf("")
	}
}

func inferColumnType(expression, dataType string) reflect.Type {
	lower := strings.ToLower(strings.TrimSpace(expression))
	switch {
	case isPureAggregateProjection(lower, "count("):
		return reflect.TypeOf(int(0))
	case strings.Contains(lower, " as signed"), strings.Contains(lower, " as integer"), strings.Contains(lower, " as int)"), strings.Contains(lower, " as int "):
		if isComputedNumericProjection(lower) {
			return reflect.TypeOf((*int)(nil))
		}
		return reflect.TypeOf(int(0))
	case strings.Contains(lower, " as bigint"):
		if isComputedNumericProjection(lower) {
			return reflect.TypeOf((*int64)(nil))
		}
		return reflect.TypeOf(int64(0))
	case strings.Contains(lower, "sum("), strings.Contains(lower, "avg("):
		return reflect.TypeOf(float64(0))
	default:
		dataType = strings.TrimSpace(dataType)
		return parseColumnType(dataType)
	}
}

func isPureAggregateProjection(expression string, aggregate string) bool {
	idx := strings.Index(expression, aggregate)
	if idx == -1 {
		return false
	}
	return strings.TrimSpace(expression[:idx]) == ""
}

func isComputedNumericProjection(expression string) bool {
	expression = strings.ToLower(strings.TrimSpace(expression))
	switch {
	case strings.Contains(expression, "count("):
		return !isPureAggregateProjection(expression, "count(")
	case strings.Contains(expression, "sum("):
		return !isPureAggregateProjection(expression, "sum(")
	case strings.Contains(expression, "avg("):
		return !isPureAggregateProjection(expression, "avg(")
	}
	return strings.Contains(expression, " + ") ||
		strings.Contains(expression, " - ") ||
		strings.Contains(expression, " * ") ||
		strings.Contains(expression, " / ") ||
		strings.Contains(expression, "case ") ||
		strings.Contains(expression, "coalesce(") ||
		strings.Contains(expression, "nullif(") ||
		strings.Contains(expression, "cast(")
}
