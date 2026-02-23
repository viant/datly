package pipeline

import (
	"reflect"
	"regexp"
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser/query"
)

var (
	criteriaBindingExpr = regexp.MustCompile(`(?i)\$criteria\.AppendBinding\([^)]*\)`)
	selectorExpr        = regexp.MustCompile(`\$\{?([a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*)\}?`)
	veltyExpr           = regexp.MustCompile(`\$\{[^}]+\}`)
	fromTableSimpleExpr = regexp.MustCompile(`(?is)\bfrom\s+([a-zA-Z_][a-zA-Z0-9_$.]*)(?:\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?`)
	braceExpr           = regexp.MustCompile(`[{}]`)
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
	if matches := fromTableSimpleExpr.FindStringSubmatch(sqlText); len(matches) > 1 {
		table := strings.Trim(matches[1], "`\"")
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
	normalized := criteriaBindingExpr.ReplaceAllString(sqlText, "1")
	normalized = veltyExpr.ReplaceAllStringFunc(normalized, func(match string) string {
		if strings.Contains(match, "sql.Insert") || strings.Contains(match, "sql.Update") || strings.Contains(match, "Nop") {
			return match
		}
		lower := strings.ToLower(match)
		if strings.Contains(lower, `build("where")`) || strings.Contains(lower, "build('where')") {
			return " WHERE 1 "
		}
		if strings.Contains(lower, `build("and")`) || strings.Contains(lower, "build('and')") {
			return " AND 1 "
		}
		return "1"
	})
	normalized = selectorExpr.ReplaceAllStringFunc(normalized, func(match string) string {
		lower := match
		if len(match) > 0 && match[0] == '$' {
			lower = match[1:]
		}
		lower = braceExpr.ReplaceAllString(lower, "")
		switch lower {
		case "sql.Insert", "sql.Update", "Nop":
			return match
		default:
			return "1"
		}
	})
	return normalized
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
