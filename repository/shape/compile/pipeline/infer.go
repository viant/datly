package pipeline

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

var nonWord = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

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

		typ := parseColumnType(column.Type)
		fields = append(fields, reflect.StructField{
			Name: fieldName,
			Type: typ,
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s,omitempty" sqlx:"name=%s"`, strings.ToLower(fieldName), columnName)),
		})
	}
	element := reflect.StructOf(fields)
	return reflect.SliceOf(element), element, "many"
}

func SanitizeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if value == strings.ToUpper(value) {
		value = strings.ToLower(value)
	}
	value = nonWord.ReplaceAllString(value, "_")
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
	value = nonWord.ReplaceAllString(strings.TrimSpace(value), "_")
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

func parseColumnType(dataType string) reflect.Type {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "", "string", "text", "varchar", "char", "uuid", "json", "jsonb":
		return reflect.TypeOf("")
	case "bool", "boolean":
		return reflect.TypeOf(false)
	case "int", "int32", "smallint", "integer":
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
