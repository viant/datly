package golang

import (
	"go/ast"
	"go/token"
)

// referenceMatcher requests native metadata/value proof from the same scoped
// framework validator. The generated owner supplies only typed graph facts.
func (e *validationEmitter) referenceMatcher() ast.Expr {
	id, a := ast.NewIdent, e.action
	reference := selectExpr(id(a.l.handlerAlias), "ValidationReference")
	method := &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{
		namedField("ctx", selectExpr(id(a.l.contextAlias), "Context")), namedField("child", id("any")), namedField("parent", id("any")), namedField("expected", reference),
	}}, Results: &ast.FieldList{List: []*ast.Field{{Type: &ast.StarExpr{X: reference}}, {Type: id("error")}}}}
	return &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{namedField("MatchReference", method)}}}
}

func (e *validationEmitter) receipts(role actionRole) []ast.Stmt {
	id, a := ast.NewIdent, e.action
	var body []ast.Stmt
	for _, relation := range a.relations(role, id("frame")) {
		parent := &ast.IndexExpr{X: selectExpr(id("actions"), relation.parent.field+"ByEntity"), Index: relation.reference}
		child := &ast.IndexExpr{X: selectExpr(id("actions"), role.field+"ByEntity"), Index: selectExpr(id("frame"), "Entity")}
		proof := []ast.Stmt{defineStmt("parent", parent), defineStmt("child", child),
			e.guard(&ast.BinaryExpr{X: &ast.BinaryExpr{X: id("parent"), Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("child"), Op: token.EQL, Y: id("nil")}}, "reference proof requires typed parent and child decisions"),
			e.guard(&ast.BinaryExpr{X: selectExpr(id("child"), "Action"), Op: token.NEQ, Y: selectExpr(id("options"), "Action")}, "reference child action differs from validated action"),
		}
		var matches []ast.Stmt
		for _, link := range relation.links {
			eligible := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(id("before"), "DeferredFields"), Op: token.NEQ, Y: id("nil")}, Op: token.LAND, Y: callExpr(selectExpr(selectExpr(id("before"), "DeferredFields"), "Has"), stringExpr(link.Child.Field))}
			expected := &ast.CompositeLit{Type: selectExpr(id(a.l.handlerAlias), "ValidationReference"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("Field"), Value: stringExpr(link.Child.Field)}, &ast.KeyValueExpr{Key: id("Table"), Value: stringExpr(relation.parent.record.plan.Table)}, &ast.KeyValueExpr{Key: id("Column"), Value: stringExpr(link.Parent.Source)}}}
			match := []ast.Stmt{
				&ast.AssignStmt{Lhs: []ast.Expr{id("matcher"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.TypeAssertExpr{X: e.member("Validator"), Type: e.referenceMatcher()}}},
				e.guard(&ast.UnaryExpr{Op: token.NOT, X: id("ok")}, "framework validator cannot resolve native reference proofs"),
				&ast.AssignStmt{Lhs: []ast.Expr{id("receipt"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("matcher"), "MatchReference"), id("ctx"), selectExpr(id("child"), "Payload"), selectExpr(id("parent"), "Payload"), expected)}}, errorGuard(id("err")),
				&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("receipt"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(id("options"), "SatisfiedReferences"), callExpr(id("append"), selectExpr(id("options"), "SatisfiedReferences"), &ast.StarExpr{X: id("receipt")}))}}},
			}
			matches = append(matches, &ast.IfStmt{Cond: eligible, Body: &ast.BlockStmt{List: match}})
		}
		bothInsert := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(id("options"), "Action"), Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteInsert")}, Op: token.LAND, Y: &ast.BinaryExpr{X: selectExpr(id("parent"), "Action"), Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteInsert")}}
		proof = append(proof, &ast.IfStmt{Cond: bothInsert, Body: &ast.BlockStmt{List: matches}})
		body = append(body, &ast.IfStmt{Cond: relation.condition, Body: &ast.BlockStmt{List: proof}})
	}
	return body
}
