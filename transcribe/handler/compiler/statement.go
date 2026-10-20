package compiler

import (
	"fmt"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/constant/identifier"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/sqlparser"
	sqlexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	sqlsource "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// CompileStatement resolves a bounded buffered service call using the
// existing Velty AST and SQL statement boundaries. Authority contains final
// canonical Go fields, never names inferred from the authored expression.
func (c *Compiler) CompileStatement(source string, authority map[string]plan.StatementSelector, constants ...*constant.Values) (*plan.StatementPlan, error) {
	root, _, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return nil, fmt.Errorf("statement program: %w", err)
	}
	var service *expr.Select
	for _, node := range root.Statements() {
		if text, ok := node.(*stmt.Append); ok && strings.TrimSpace(text.Append) == "" {
			continue
		}
		selector, ok := node.(*expr.Select)
		if !ok || service != nil {
			return nil, fmt.Errorf("statement program requires exactly one direct DML call")
		}
		service = selector
	}
	if service == nil || service.ID != "dml" {
		return nil, fmt.Errorf("statement program requires dml.Execute, dml.ExecuteWithResult, dml.Merge, dml.Insert or dml.Delete")
	}
	method, ok := service.X.(*expr.Select)
	if !ok || (method.ID != "ExecuteWithResult" && method.ID != "Execute" && method.ID != "Merge" && method.ID != "Insert" && method.ID != "Delete" && method.ID != "InsertIgnore") {
		return nil, fmt.Errorf("statement program requires dml.Execute, dml.ExecuteWithResult, dml.Merge, dml.Insert or dml.Delete")
	}
	if method.ID == "Insert" || method.ID == "Delete" || method.ID == "InsertIgnore" {
		return compileBatchStatement(method, authority)
	}
	if method.ID == "Merge" {
		return compileMergeStatement(method, authority)
	}
	argumentOffset := 1
	if method.ID == "ExecuteWithResult" {
		argumentOffset = 2
	}
	call, ok := method.X.(*expr.Call)
	if !ok || call.X != nil || len(call.Args) < argumentOffset {
		return nil, fmt.Errorf("%s requires static SQL and its destination/argument selectors", method.ID)
	}
	literal, ok := call.Args[0].(*expr.Literal)
	if !ok || literal.RType != reflect.TypeFor[string]() {
		return nil, fmt.Errorf("statement SQL must be a string literal")
	}
	sqlText := literal.Value
	var constantNames []string
	if len(constants) > 1 {
		return nil, fmt.Errorf("statement accepts one immutable constant set")
	}
	if len(constants) == 1 && constants[0] != nil {
		renderer := identifier.Renderer{Values: constants[0]}
		refs, err := renderer.References(sqlText)
		if err != nil {
			return nil, fmt.Errorf("statement constants: %w", err)
		}
		seen := map[string]bool{}
		for _, ref := range refs {
			if !seen[ref.Name] {
				constantNames = append(constantNames, ref.Name)
				seen[ref.Name] = true
			}
		}
		sqlText, err = renderer.Render(sqlText)
		if err != nil {
			return nil, fmt.Errorf("statement constants: %w", err)
		}
	}

	if err := sqlsource.ValidateStructure(sqlText); err != nil {
		return nil, fmt.Errorf("statement SQL: %w", err)
	}
	kind := sqlparser.ParseKind(sqlText)
	if staticStatementCount(sqlText) != 1 {
		return nil, fmt.Errorf("statement SQL requires one static executable statement")
	}
	// LastInsertId is an insertion result. DDL, transaction control, and other
	// statement kinds cannot acquire this capability through authoring.
	if method.ID == "ExecuteWithResult" && !kind.IsInsert() {
		return nil, fmt.Errorf("LastInsertId requires an INSERT statement")
	}
	switch kind {
	case sqlparser.KindInsert:
		parsed, err := sqlparser.ParseInsert(sqlText)
		if err != nil {
			return nil, fmt.Errorf("statement INSERT: %w", err)
		}
		if method.ID == "ExecuteWithResult" && (len(parsed.Columns) == 0 || len(parsed.Values) != len(parsed.Columns)) {
			return nil, fmt.Errorf("statement result requires one explicit INSERT row; batching is unsupported")
		}
	case sqlparser.KindUpdate:
		if _, err := sqlparser.ParseUpdate(sqlText); err != nil {
			return nil, fmt.Errorf("statement UPDATE: %w", err)
		}
	case sqlparser.KindDelete:
		if _, err := sqlparser.ParseDelete(sqlText); err != nil {
			return nil, fmt.Errorf("statement DELETE: %w", err)
		}
	default:
		return nil, fmt.Errorf("buffered statement requires INSERT, UPDATE or DELETE")
	}
	bindings := 0
	scanner := sqlsource.NewCodeScanner(sqlText, 0)
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		switch sqlText[pos] {
		case '$', '#':
			return nil, fmt.Errorf("statement SQL requires static executable text")
		}
	}
	bindings = sqlx.ParseParameters(sqlText).PositionalCount()
	if bindings != len(call.Args)-argumentOffset {
		return nil, fmt.Errorf("statement has %d placeholders but %d arguments", bindings, len(call.Args)-argumentOffset)
	}
	result := &plan.StatementPlan{SQL: literal.Value, Method: method.ID, SQLConstants: constantNames}
	if method.ID == "ExecuteWithResult" {
		dest, err := resolveStatementSelector(call.Args[1], authority)
		if err != nil {
			return nil, fmt.Errorf("result destination: %w", err)
		}
		if !dest.Addressable || !signedStatementInteger(dest.Type.Name) {
			return nil, fmt.Errorf("result destination must be an addressable signed integer field")
		}
		result.LastInsertID = dest
	}
	for i, arg := range call.Args[argumentOffset:] {
		field, err := resolveStatementSelector(arg, authority)
		if err != nil {
			return nil, fmt.Errorf("statement argument %d: %w", i+1, err)
		}
		if field.Type.Name == "[]int" {
			if method.ID != "Execute" || (kind != sqlparser.KindUpdate && kind != sqlparser.KindDelete) || !statementCollectionPosition(sqlText, i) {
				return nil, fmt.Errorf("statement argument %d must be a typed scalar field or a []int binding in a positive UPDATE or DELETE IN predicate", i+1)
			}
			result.CollectionOrdinals = append(result.CollectionOrdinals, i)
		} else if !StatementScalarType(field.Type.Name) {
			return nil, fmt.Errorf("statement argument %d must be a typed scalar field", i+1)
		}
		result.Arguments = append(result.Arguments, field)
	}
	return result, nil
}

// staticStatementCount uses SQLParser's protected-region lexer for boundaries.
// The final whitespace lets PreviousSignificant include a trailing quoted
// region while excluding trailing comments. Parenthesized SQL remains one unit.
func staticStatementCount(text string) int {
	source := text + "\n "
	scanner := sqlsource.NewCodeScanner(source, 0)
	count, boundary, depth := 0, -1, 0
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		switch source[pos] {
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		case ';':
			if depth == 0 {
				if previous, _ := scanner.PreviousSignificant(); previous > boundary {
					count++
				}
				boundary = pos
			}
		}
		if pos == len(source)-1 {
			if previous, _ := scanner.PreviousSignificant(); previous > boundary {
				count++
			}
		}
	}
	return count
}

func resolveStatementSelector(value any, authority map[string]plan.StatementSelector) (plan.StatementSelector, error) {
	parts, err := statementSelectorPath(value)
	if err != nil {
		return plan.StatementSelector{}, err
	}
	key := strings.Join(parts, ".")
	field, ok := authority[key]
	if !ok || len(field.Path) < 2 || (field.Path[0] != "Input" && field.Path[0] != "Output") {
		return plan.StatementSelector{}, fmt.Errorf("unresolved canonical selector %q", key)
	}
	for _, part := range field.Path {
		if !token.IsIdentifier(part) || token.Lookup(part).IsKeyword() {
			return plan.StatementSelector{}, fmt.Errorf("invalid canonical field path")
		}
	}
	field.Path = append(plan.FieldPath(nil), field.Path...)
	return field, nil
}

func signedStatementInteger(name string) bool {
	switch name {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

// StatementScalarType reports the existing scalar argument boundary for canonical metadata consumers.
func StatementScalarType(name string) bool {
	// Nullable arguments retain their pointer selectors for database/sql. Strip
	// exactly one level; pointer results and non-scalar pointees stay invalid.
	name = strings.TrimPrefix(name, "*")
	if signedStatementInteger(name) {
		return true
	}
	switch name {
	case "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "bool", "string", "time.Time":
		return true
	}
	return false
}

// Mark positional placeholders through SQLX so protected text and named parameters
// cannot shift their ordinals. Only an IN predicate with a single placeholder
// value and scalar identifier left operand grants collection authority.
func statementCollectionPosition(sql string, ordinal int) bool {
	marker := "datlyNativeCollection"
	for strings.Contains(sql, marker) {
		marker += "_"
	}
	marked := sqlx.ParseParameters(sql).RewritePositional(func(i int) string {
		if i == ordinal {
			return ":" + marker
		}
		return "?"
	})
	var qualify *sqlexpr.Qualify
	switch sqlparser.ParseKind(marked) {
	case sqlparser.KindUpdate:
		parsed, err := sqlparser.ParseUpdate(marked)
		if err != nil {
			return false
		}
		qualify = parsed.Qualify
	case sqlparser.KindDelete:
		parsed, err := sqlparser.ParseDelete(marked)
		if err != nil {
			return false
		}
		qualify = parsed.Qualify
	default:
		return false
	}
	if qualify == nil {
		return false
	}
	var visit func(node.Node) bool
	visit = func(n node.Node) bool {
		switch actual := n.(type) {
		case *sqlexpr.Parenthesis:
			return visit(actual.X)
		case *sqlexpr.Binary:
			// A wholly parenthesized WHERE is represented by the parser as a
			// transparent binary shell around its parenthesis node.
			if actual.Op == "" && actual.Y == nil {
				if _, ok := actual.X.(*sqlexpr.Parenthesis); ok {
					return visit(actual.X)
				}
			}
			if strings.EqualFold(actual.Op, "AND") || strings.EqualFold(actual.Op, "OR") {
				return visit(actual.X) || visit(actual.Y)
			}
			if !strings.EqualFold(actual.Op, "IN") || sqlexpr.Identity(actual.X) == nil {
				return false
			}
			right, ok := actual.Y.(*sqlexpr.Parenthesis)
			if !ok {
				return false
			}
			placeholder, ok := right.X.(*sqlexpr.Placeholder)
			return ok && strings.TrimPrefix(placeholder.Name, ":") == marker
		}
		return false
	}
	return visit(qualify.X)
}

// StatementSelectorPaths returns only static selector arguments referenced by a
// direct statement call. It grants no authority; CompileStatement still validates
// the complete program and every resolved selector.
func StatementSelectorPaths(source string) []plan.FieldPath {
	root, _, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return nil
	}
	var paths []plan.FieldPath
	for _, node := range root.Statements() {
		service, ok := node.(*expr.Select)
		if !ok || service.ID != "dml" {
			continue
		}
		method, ok := service.X.(*expr.Select)
		if !ok {
			continue
		}
		call, ok := method.X.(*expr.Call)
		if !ok {
			continue
		}
		for _, argument := range call.Args {
			if path, err := statementSelectorPath(argument); err == nil {
				paths = append(paths, path)
			}
		}
	}
	return paths
}

func statementSelectorPath(value any) (plan.FieldPath, error) {
	selector, ok := value.(*expr.Select)
	if !ok {
		return nil, fmt.Errorf("a canonical field selector is required")
	}
	var parts []string
	for selector != nil {
		if !token.IsIdentifier(selector.ID) || token.Lookup(selector.ID).IsKeyword() {
			return nil, fmt.Errorf("invalid selector %q", selector.ID)
		}
		parts = append(parts, selector.ID)
		if selector.X == nil {
			break
		}
		selector, ok = selector.X.(*expr.Select)
		if !ok {
			return nil, fmt.Errorf("calls, indexing and dynamic selectors are unsupported")
		}
	}
	return parts, nil
}
