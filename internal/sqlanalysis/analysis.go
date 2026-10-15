package sqlanalysis

import (
	"fmt"
	"reflect"
	"strings"

	sqlmacro "github.com/viant/datly/sql/macro"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqlsource "github.com/viant/sqlparser/source"
	veltyexpr "github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

// InputValidator checks declared value operands without evaluating template code.
type InputValidator func(*veltyexpr.Select) (string, error)

// SQL returns an analysis-only copy. Predicate bodies never supply column or
// table authority, and no template code is executed here.
func SQL(source string, validate InputValidator) (string, error) {
	text, _, err := prepare(source, validate)
	return text, err
}

// Parse retains the checked top-level AST, so consumers do not parse the same
// predicate-masked statement a second time after scope validation.
func Parse(source string, validate InputValidator) (string, *query.Select, error) {
	return parse(source, validate, false)
}

// Checked retains complete checked nested query ASTs for source-invariant
// projection consumers. It does not establish a particular projection's
// eligibility; the consumer must still prove a complete SELECT output contract.
func Checked(source string, validate InputValidator) (string, *query.Select, error) {
	return parse(source, validate, true)
}

func parse(source string, validate InputValidator, checkPlain bool) (string, *query.Select, error) {
	text, parsed, err := prepare(source, validate)
	if err != nil {
		return "", nil, err
	}
	if parsed == nil {
		parsed, err = sqlparser.ParseQuery(unwrapAnalysisSQL(text), sqlparser.WithStructuralValidation())
		if err == nil && checkPlain && parsed != nil {
			err = validateAnalysisPredicates(parsed, nil, nil)
		}
	}
	return text, parsed, err
}

func prepare(SQL string, validate InputValidator) (string, *query.Select, error) {
	// Parent-key calls are relation predicates, not projection identity. Use
	// the canonical macro parser only in this detached analysis copy.
	var err error
	SQL, _, err = sqlmacro.StripParentKeyCalls(SQL)
	if err != nil {
		return "", nil, err
	}

	prefix := "__datly_analysis_predicate_"
	for strings.Contains(SQL, prefix) {
		prefix += "_"
	}
	markers := map[string]string{}
	values := map[string]string{}
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
			return "", nil, fmt.Errorf("unsupported dynamic SQL structure at byte %d: Unsafe SQL fragments have no static identity", pos)
		}
		if !strings.HasPrefix(SQL[pos:], "${") && !strings.HasPrefix(SQL[pos:], "$predicate.") {
			continue
		}
		cursor := parsly.NewCursor("", input, 0)
		cursor.Pos = pos + 1
		selector, err := veltyparser.MatchSelector(cursor)
		if err != nil {
			return "", nil, fmt.Errorf("malformed SQL template at byte %d: %w", pos, err)
		}
		if selector.ID != "predicate" {
			if validate == nil {
				return "", nil, fmt.Errorf("unsupported dynamic SQL structure at byte %d: input declaration context is unavailable", pos)
			}
			path, err := validate(selector)
			if err != nil {
				return "", nil, fmt.Errorf("static SQL input at byte %d: %w", pos, err)
			}
			marker := fmt.Sprintf(":%svalue_%d", prefix, len(values))
			values[marker] = path
			result.WriteString(SQL[previous:pos])
			result.WriteString(marker)
			previous = cursor.Pos
			continue
		}
		keyword, err := predicateAnalysisKeyword(selector)
		if err != nil {
			return "", nil, fmt.Errorf("unsupported dynamic SQL structure at byte %d: %w", pos, err)
		}
		marker := fmt.Sprintf("%s%d", prefix, len(markers))
		markers[marker] = keyword
		result.WriteString(SQL[previous:pos])
		result.WriteString(" " + keyword + " (" + marker + " = 1) ")
		previous = cursor.Pos
	}
	if !hasTemplate {
		return SQL, nil, nil
	}
	result.WriteString(SQL[previous:])
	analysis := result.String()
	parsed, err := sqlparser.ParseQuery(unwrapAnalysisSQL(analysis), sqlparser.WithStructuralValidation())
	if err != nil {
		return "", nil, fmt.Errorf("parse static SQL with predicate clauses: %w", err)
	}
	if err := validateAnalysisPredicates(parsed, markers, values); err != nil {
		return "", nil, err
	}
	return analysis, parsed, nil
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

func validateAnalysisPredicates(parsed *query.Select, markers map[string]string, values map[string]string) error {
	seen := map[string]bool{}
	seenValues := map[string]bool{}
	var conditions func(node.Node, string)
	conditions = func(value node.Node, clause string) {
		sqlparser.Traverse(value, func(n node.Node) bool {
			switch actual := n.(type) {
			case []node.Node:
				for _, child := range actual {
					conditions(child, clause)
				}
				return false
			case *expr.Binary:
				// Traverse stops a binary sibling when its left visitor returns
				// false. Visit both operands explicitly across CASE boundaries.
				conditions(actual.X, clause)
				conditions(actual.Y, clause)
				return false
			case *expr.Switch:
				// Only searched WHEN expressions are predicate conditions. A
				// simple CASE selector/values and all results remain operands.
				simple := len(actual.Cases) > 0 && actual.Cases[0] != nil && actual.Cases[0].Y == nil
				for _, branch := range actual.Cases {
					if branch == nil {
						continue
					}
					scope := "CASE"
					if !simple && branch.Y != nil {
						scope = "WHEN"
					}
					conditions(branch.X.X, scope)
					conditions(branch.Y, "CASE")
				}
				return false
			case *query.Select, *expr.Raw:
				// A subquery has its own clause boundaries.
				return false
			case *expr.Ident:
				if keyword, ok := markers[actual.Name]; ok && (clause == "WHERE" || clause == "HAVING" || clause == "ON" || clause == "WHEN") && (keyword != "WHERE" && keyword != "HAVING" || keyword == clause) {
					seen[actual.Name] = true
				}
			case *expr.Placeholder:
				if _, ok := values[actual.Name]; ok {
					seenValues[actual.Name] = true
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
				conditions(actual.QualifyClause, "QUALIFY")
				for _, join := range actual.Joins {
					if walkErr = validateAnalysisSource(join.With); walkErr != nil {
						return false
					}
					conditions(join.On, "ON")
				}
				for _, item := range actual.List {
					if item != nil && item.Alias != "" {
						// A fixed alias names an output slot; the operand never
						// supplies source-column authority.
						conditions(item.Expr, "SELECT")
					}
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
					with.X = nested
				}
			case *expr.Raw:
				raw := unwrapAnalysisSQL(actual.Raw)
				if isAnalysisQuery(raw) {
					nested, err := sqlparser.ParseQuery(raw, sqlparser.WithStructuralValidation())
					if err != nil {
						walkErr = err
					} else {
						walkErr = walk(nested, depth+1)
						if walkErr == nil {
							actual.X = nested
						}
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
			return fmt.Errorf("unsupported dynamic SQL structure: predicate template must occur in a WHERE, HAVING, JOIN ON or searched CASE WHEN condition")
		}
	}
	for marker, path := range values {
		if !seenValues[marker] {
			return fmt.Errorf("unsupported dynamic SQL structure: input %s must occur in a value operand, not a table, column or clause identity", path)
		}
	}
	return nil
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
