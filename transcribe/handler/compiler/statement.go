package compiler

import (
	"fmt"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/transcribe/dql/statement"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/sqlparser"
	sqlsource "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// CompileStatement resolves a bounded result-bearing service call using the
// existing Velty AST and SQL statement boundaries. Authority contains final
// canonical Go fields, never names inferred from the authored expression.
func (c *Compiler) CompileStatement(source string, authority map[string]plan.StatementSelector) (*plan.StatementPlan, error) {
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
		return nil, fmt.Errorf("statement program requires dml.ExecuteWithResult")
	}
	method, ok := service.X.(*expr.Select)
	if !ok || method.ID != "ExecuteWithResult" {
		return nil, fmt.Errorf("statement program requires dml.ExecuteWithResult")
	}
	call, ok := method.X.(*expr.Call)
	if !ok || call.X != nil || len(call.Args) < 2 {
		return nil, fmt.Errorf("ExecuteWithResult requires static SQL, result destination and ordered argument selectors")
	}
	literal, ok := call.Args[0].(*expr.Literal)
	if !ok || literal.RType != reflect.TypeFor[string]() {
		return nil, fmt.Errorf("statement SQL must be a string literal")
	}
	sqlText := literal.Value
	if err := sqlsource.ValidateStructure(sqlText); err != nil {
		return nil, fmt.Errorf("statement SQL: %w", err)
	}
	spans := statement.Parse(sqlText)
	if len(spans) != 1 || spans[0].Kind != statement.KindExec {
		return nil, fmt.Errorf("statement SQL requires one static executable statement")
	}
	// LastInsertId is an insertion result. DDL, transaction control, and other
	// statement kinds cannot acquire this capability through authoring.
	if !strings.EqualFold(spans[0].Operation, "INSERT") {
		return nil, fmt.Errorf("LastInsertId requires an INSERT statement")
	}
	parsed, err := sqlparser.ParseInsert(sqlText)
	if err != nil {
		return nil, fmt.Errorf("statement INSERT: %w", err)
	}
	if len(parsed.Columns) == 0 || len(parsed.Values) != len(parsed.Columns) {
		return nil, fmt.Errorf("statement result requires one explicit INSERT row; batching is unsupported")
	}
	bindings := 0
	scanner := sqlsource.NewCodeScanner(sqlText, 0)
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		switch sqlText[pos] {
		case '$', '#':
			return nil, fmt.Errorf("statement SQL requires static executable text")
		case '?':
			bindings++
		}
	}
	if bindings != len(call.Args)-2 {
		return nil, fmt.Errorf("statement has %d placeholders but %d arguments", bindings, len(call.Args)-2)
	}
	dest, err := resolveStatementSelector(call.Args[1], authority)
	if err != nil {
		return nil, fmt.Errorf("result destination: %w", err)
	}
	if !dest.Addressable || !signedStatementInteger(dest.Type.Name) {
		return nil, fmt.Errorf("result destination must be an addressable signed integer field")
	}
	result := &plan.StatementPlan{SQL: sqlText, LastInsertID: dest}
	for i, arg := range call.Args[2:] {
		field, err := resolveStatementSelector(arg, authority)
		if err != nil {
			return nil, fmt.Errorf("statement argument %d: %w", i+1, err)
		}
		if !statementScalar(field.Type.Name) {
			return nil, fmt.Errorf("statement argument %d must be a typed scalar field", i+1)
		}
		result.Arguments = append(result.Arguments, field)
	}
	return result, nil
}

func resolveStatementSelector(value any, authority map[string]plan.StatementSelector) (plan.StatementSelector, error) {
	selector, ok := value.(*expr.Select)
	if !ok {
		return plan.StatementSelector{}, fmt.Errorf("a canonical field selector is required")
	}
	var parts []string
	for selector != nil {
		if !token.IsIdentifier(selector.ID) || token.Lookup(selector.ID).IsKeyword() {
			return plan.StatementSelector{}, fmt.Errorf("invalid selector %q", selector.ID)
		}
		parts = append(parts, selector.ID)
		if selector.X == nil {
			break
		}
		selector, ok = selector.X.(*expr.Select)
		if !ok {
			return plan.StatementSelector{}, fmt.Errorf("calls, indexing and dynamic selectors are unsupported")
		}
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
func statementScalar(name string) bool {
	if signedStatementInteger(name) {
		return true
	}
	switch name {
	case "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "bool", "string", "time.Time":
		return true
	}
	return false
}
