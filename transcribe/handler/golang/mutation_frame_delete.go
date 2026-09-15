package golang

import (
	"go/ast"
	"go/token"
	"strings"
)

// deletionScope keeps delete authorization parent-scoped even when ordinary
// updates explicitly allow reparenting on the same relation.
func (e *frameEmitter) deletionScope(record *recordLowering) ([]ast.Stmt, ast.Expr, error) {
	id := ast.NewIdent
	for _, field := range record.plan.Entity.Fields {
		if !field.DeleteMarker {
			continue
		}
		path := strings.Join((&entityEmitter{l: e.l}).entityFieldPath(record, field.Name), ".")
		body := []ast.Stmt{e.accessorAssignment(parseExpr(record.value.base), path, "deleteScopeAccess"), e.visitError(), &ast.AssignStmt{Lhs: []ast.Expr{id("deleteScopeValue"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("deleteScopeAccess"), "Get"), selectExpr(id("original"), "original"))}}, e.visitError()}
		value := ast.Expr(id("deleteScopeValue"))
		var requested ast.Expr = callExpr(selectExpr(id("original"), "Has"), stringExpr(field.Name))
		if field.Type.Pointer || strings.HasPrefix(field.Type.Name, "*") {
			requested = &ast.BinaryExpr{X: requested, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(value, "IsNil"))}}
			value = callExpr(selectExpr(value, "Elem"))
		}
		requested = &ast.BinaryExpr{X: requested, Op: token.LAND, Y: callExpr(selectExpr(value, "Bool"))}
		body = append(body, defineStmt("deleteScope", requested))
		return body, id("deleteScope"), nil
	}
	return nil, id("false"), nil
}
