package pipeline

// read.go — SELECT compilation: parses DQL into a plan.View using
// multi-strategy parse with template-signal fallback.
// SQL normalization and token utilities live in read_normalize.go.

import (
	"reflect"
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser/query"
)

// BuildRead compiles a SELECT DQL fragment into a plan.View.
// It applies multiple parse strategies and gracefully degrades to a
// loose (schema-less) view for template-driven SQL that cannot be fully parsed.
func BuildRead(sourceName, sqlText string) (*plan.View, []*dqlshape.Diagnostic, error) {
	queryNode, parseDiag, parserSQL, err := resolveQueryNode(sqlText)

	// Template-driven SQL may legitimately fail strict parsing; treat as warning.
	if (err != nil || parseDiag != nil) && hasTemplateSignals(sqlText) {
		if parseDiag != nil {
			parseDiag.Severity = dqlshape.SeverityWarning
		}
		return buildLooseRead(sourceName, sqlText), collectDiags(parseDiag), nil
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

// collectDiags returns a single-element slice for a non-nil diagnostic,
// or nil otherwise. Used to avoid repeated nil checks at call sites.
func collectDiags(diag *dqlshape.Diagnostic) []*dqlshape.Diagnostic {
	if diag == nil {
		return nil
	}
	return []*dqlshape.Diagnostic{diag}
}
