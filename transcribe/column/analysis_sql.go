package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqlsource "github.com/viant/sqlparser/source"
	veltyexpr "github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

// projectionAnalysisSQL produces an analysis-only copy. Predicate bodies never
// supply column or table authority, and no template code is executed here.
func projectionAnalysisSQL(SQL string) (string, error) {
	prefix := "__datly_analysis_predicate_"
	for strings.Contains(SQL, prefix) {
		prefix += "_"
	}
	markers := map[string]string{}
	var result strings.Builder
	previous := 0
	hasTemplate := false
	input := []byte(SQL)
	scanner := sqlsource.NewCodeScanner(SQL, 0)
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if pos < previous || SQL[pos] != '$' {
			continue
		}
		hasTemplate = true
		if strings.HasPrefix(SQL[pos:], "$Unsafe.") {
			return "", fmt.Errorf("unsupported dynamic SQL structure at byte %d: Unsafe SQL fragments have no static identity", pos)
		}
		if !strings.HasPrefix(SQL[pos:], "${") && !strings.HasPrefix(SQL[pos:], "$predicate.") {
			continue
		}
		cursor := parsly.NewCursor("", input, 0)
		cursor.Pos = pos + 1
		selector, err := veltyparser.MatchSelector(cursor)
		if err != nil {
			return "", fmt.Errorf("malformed SQL template at byte %d: %w", pos, err)
		}
		if selector.ID != "predicate" {
			// Ordinary input operands remain SQLParser's responsibility. Braced
			// SQL fragments cannot establish a static projection or source.
			if strings.HasPrefix(SQL[pos:], "${") {
				return "", fmt.Errorf("unsupported dynamic SQL structure at byte %d: only predicate clauses can be analyzed", pos)
			}
			continue
		}
		keyword, err := predicateAnalysisKeyword(selector)
		if err != nil {
			return "", fmt.Errorf("unsupported dynamic SQL structure at byte %d: %w", pos, err)
		}
		marker := fmt.Sprintf("%s%d", prefix, len(markers))
		markers[marker] = keyword
		result.WriteString(SQL[previous:pos])
		result.WriteString(" " + keyword + " (" + marker + " = 1) ")
		previous = cursor.Pos
	}
	if !hasTemplate {
		return SQL, nil
	}
	result.WriteString(SQL[previous:])
	analysis := result.String()
	parsed, err := sqlparser.ParseQuery(unwrapAnalysisSQL(analysis), sqlparser.WithStructuralValidation())
	if err != nil {
		return "", fmt.Errorf("parse static SQL with predicate clauses: %w", err)
	}
	if err := validateAnalysisPredicates(parsed, markers); err != nil {
		return "", err
	}
	return analysis, nil
}

func predicateAnalysisKeyword(selector *veltyexpr.Select) (string, error) {
	method, ok := selector.X.(*veltyexpr.Select)
	if !ok {
		return "", fmt.Errorf("predicate requires a recognized condition expression")
	}
	call, ok := method.X.(*veltyexpr.Call)
	if !ok {
		return "", fmt.Errorf("predicate method %s requires a call", method.ID)
	}
	if call.X == nil {
		switch method.ID {
		case "FilterGroup", "ExpandWith":
			if len(call.Args) == 2 {
				return "", nil
			}
		case "Expand":
			if len(call.Args) == 1 {
				return "", nil
			}
		}
	}
	if method.ID != "Builder" || len(call.Args) != 0 {
		return "", fmt.Errorf("unsupported predicate method %s", method.ID)
	}
	for call.X != nil {
		method, ok = call.X.(*veltyexpr.Select)
		if !ok {
			break
		}
		call, ok = method.X.(*veltyexpr.Call)
		if !ok {
			break
		}
		switch method.ID {
		case "Combine", "CombineAnd", "CombineOr":
			continue
		case "And", "Or":
			if len(call.Args) != 0 {
				return "", fmt.Errorf("predicate %s takes no arguments", method.ID)
			}
		case "Build":
			if len(call.Args) == 1 && call.X == nil {
				literal, ok := call.Args[0].(*veltyexpr.Literal)
				if ok && literal.RType == reflect.TypeFor[string]() {
					keyword := strings.ToUpper(strings.TrimSpace(literal.Value))
					switch keyword {
					case "", "WHERE", "HAVING", "AND", "OR":
						return keyword, nil
					}
				}
			}
			return "", fmt.Errorf("predicate Build requires a static WHERE, HAVING, AND, OR or empty keyword")
		default:
			return "", fmt.Errorf("unsupported predicate builder method %s", method.ID)
		}
	}
	return "", fmt.Errorf("predicate builder must end in Build")
}

func validateAnalysisPredicates(parsed *query.Select, markers map[string]string) error {
	seen := map[string]bool{}
	var conditions func(node.Node, string)
	conditions = func(value node.Node, clause string) {
		sqlparser.Traverse(value, func(n node.Node) bool {
			switch actual := n.(type) {
			case []node.Node:
				for _, child := range actual {
					conditions(child, clause)
				}
				return false
			case *query.Select, *expr.Raw:
				// A subquery has its own clause boundaries.
				return false
			case *expr.Ident:
				if keyword, ok := markers[actual.Name]; ok && (keyword != "WHERE" && keyword != "HAVING" || keyword == clause) {
					seen[actual.Name] = true
				}
			}
			return true
		})
	}
	var walk func(node.Node, int) error
	walk = func(value node.Node, depth int) error {
		if depth > 32 {
			return fmt.Errorf("static SQL projection is recursive")
		}
		var walkErr error
		sqlparser.Traverse(value, func(n node.Node) bool {
			if walkErr != nil {
				return false
			}
			switch actual := n.(type) {
			case *query.Select:
				if walkErr = validateAnalysisSource(actual.From.X); walkErr != nil {
					return false
				}
				conditions(actual.Qualify, "WHERE")
				conditions(actual.Having, "HAVING")
				for _, join := range actual.Joins {
					if walkErr = validateAnalysisSource(join.With); walkErr != nil {
						return false
					}
					conditions(join.On, "ON")
				}
				for _, item := range actual.List {
					if item != nil && item.Alias == "" && sqlsource.Token("$").Contains(sqlparser.Stringify(item.Expr)) {
						walkErr = fmt.Errorf("unsupported dynamic SQL structure: projection has no static output name")
						return false
					}
				}
				// SQLParser's generic traversal does not visit CTE declarations.
				for _, with := range actual.WithSelects {
					nested, err := sqlparser.ParseQuery(unwrapAnalysisSQL(with.Raw), sqlparser.WithStructuralValidation())
					if err != nil {
						walkErr = err
						return false
					}
					if walkErr = walk(nested, depth+1); walkErr != nil {
						return false
					}
				}
			case *expr.Raw:
				raw := unwrapAnalysisSQL(actual.Raw)
				if isAnalysisQuery(raw) {
					nested, err := sqlparser.ParseQuery(raw, sqlparser.WithStructuralValidation())
					if err != nil {
						walkErr = err
					} else {
						walkErr = walk(nested, depth+1)
					}
				}
				return false
			case []node.Node:
				for _, child := range actual {
					if walkErr = walk(child, depth+1); walkErr != nil {
						break
					}
				}
				return false
			}
			return true
		})
		return walkErr
	}
	if err := walk(parsed, 0); err != nil {
		return fmt.Errorf("parse static SQL predicate scope: %w", err)
	}
	for marker := range markers {
		if !seen[marker] {
			return fmt.Errorf("unsupported dynamic SQL structure: predicate template must occur in a WHERE, HAVING or JOIN ON condition")
		}
	}
	return nil
}

func parseProjectionAnalysis(SQL string) (*query.Select, error) {
	analysis, err := projectionAnalysisSQL(SQL)
	if err != nil {
		return nil, err
	}
	return sqlparser.ParseQuery(unwrapAnalysisSQL(analysis), sqlparser.WithStructuralValidation())
}

func unwrapAnalysisSQL(SQL string) string {
	for {
		SQL = strings.TrimSpace(SQL)
		group, end, ok := sqlsource.ReadGroupString(SQL, 0, '(', ')')
		if !ok || end != len(SQL) {
			return SQL
		}
		SQL = group[1 : len(group)-1]
	}
}

func isAnalysisQuery(SQL string) bool {
	scanner := sqlsource.NewCodeScanner(SQL, 0)
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if sqlsource.IsWhitespace(SQL[pos]) {
			continue
		}
		upper := strings.ToUpper(SQL[pos:])
		return sqlsource.FindCodeToken(upper, "SELECT", 0) == 0 || sqlsource.FindCodeToken(upper, "WITH", 0) == 0
	}
	return false
}

func validateAnalysisSource(value node.Node) error {
	if value == nil {
		return nil
	}
	SQL := sqlparser.Stringify(value)
	if !isAnalysisQuery(unwrapAnalysisSQL(SQL)) && sqlsource.Token("$").Contains(SQL) {
		return fmt.Errorf("unsupported dynamic SQL structure: source has no static table identity")
	}
	return nil
}
