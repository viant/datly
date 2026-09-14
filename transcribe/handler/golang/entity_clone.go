package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func (e *entityEmitter) cloneOptionsDeclaration() (ast.Decl, error) {
	typ := selectExpr(ast.NewIdent(e.shapeAlias), "CloneOptions")
	options := ast.NewIdent("options")
	body := []ast.Stmt{defineStmt("options", &ast.CompositeLit{Type: typ})}
	for _, record := range e.l.records {
		if !e.enabled(record) {
			continue
		}
		entityType := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.reflectAlias), "TypeOf"), callExpr(record.value.pointerExpr(), ast.NewIdent("nil"))), "Elem"))
		paths := []ast.Expr{entityType}
		for _, field := range record.plan.Entity.Fields {
			if !field.Relation || field.Writable {
				paths = append(paths, stringExpr(strings.Join(e.entityFieldPath(record, field.Name), ".")))
			}
		}
		relations, err := e.entityRelations(record)
		if err != nil {
			return nil, err
		}
		for _, relation := range relations {
			paths = append(paths, stringExpr(strings.Join(relation.holder, ".")))
		}
		if marker := record.plan.Entity.MarkerField; marker != "" {
			for _, field := range record.plan.Entity.Fields {
				paths = append(paths, stringExpr(marker+"."+field.Name))
			}
		}
		body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(options, "Select"), paths...)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.CompositeLit{Type: typ}, ast.NewIdent("err"))}}})
	}
	body = append(body, returnStmt(options, ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent(e.prefix + "CloneOptions"), Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: typ}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}
func (e *entityEmitter) captureSelectedBaseline(snapshot, values ast.Expr) ([]ast.Stmt, error) {
	record := e.l.recordByPlan[e.l.plan.Root]
	clone := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shapeAlias), "Runtime")}}, "CloneValue"), values, selectExpr(snapshot, "options"))
	body := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("err"))}}}, defineStmt("originalValues", &ast.TypeAssertExpr{X: ast.NewIdent("cloned"), Type: parseExpr(record.value.expression())})}
	body = append(body, e.attachValues(ast.NewIdent("originalValues"), selectExpr(snapshot, "Roots"), record, snapshot)...)
	return body, nil
}
func (e *entityEmitter) attachDeclaration(record *recordLowering) (ast.Decl, error) {
	state, value := ast.NewIdent("state"), ast.NewIdent("value")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: &ast.BinaryExpr{X: value, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: selectExpr(state, "attached")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt()}}}, assignStmt(selectExpr(state, "attached"), ast.NewIdent("true")), assignStmt(selectExpr(state, "original"), value)}
	relations, err := e.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for index, relation := range relations {
		name := "relation" + strconv.Itoa(index)
		body = append(body, e.captureRead(selectExpr(ast.NewIdent("s"), e.accessName(record, "Relation", relation.storage)), value, name, returnStmt())...)
		holder := e.reflectedRelationValue(ast.NewIdent(name), relation.child)
		body = append(body, &ast.IfStmt{Cond: ast.NewIdent(name + "Present"), Body: &ast.BlockStmt{List: e.attachValues(holder, selectExpr(state, relation.storage), relation.child, ast.NewIdent("s"))}})
	}
	return &ast.FuncDecl{Name: ast.NewIdent("attach" + strconv.Itoa(record.order)), Recv: &ast.FieldList{List: []*ast.Field{namedField("s", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record)), namedField("value", record.value.pointerExpr())}}}, Body: &ast.BlockStmt{List: body}}, nil
}
func (e *entityEmitter) attachValues(values, states ast.Expr, record *recordLowering, receiver ast.Expr) []ast.Stmt {
	var value ast.Expr = values
	var state ast.Expr = &ast.IndexExpr{X: states, Index: &ast.BasicLit{Kind: token.INT, Value: "0"}}
	if record.value.many {
		value = &ast.IndexExpr{X: values, Index: ast.NewIdent("index")}
		state = &ast.IndexExpr{X: states, Index: ast.NewIdent("index")}
	}
	if !record.value.pointer {
		value = &ast.UnaryExpr{Op: token.AND, X: value}
	}
	call := &ast.ExprStmt{X: callExpr(selectExpr(receiver, "attach"+strconv.Itoa(record.order)), state, value)}
	if record.value.many {
		return []ast.Stmt{&ast.RangeStmt{Key: ast.NewIdent("index"), Tok: token.DEFINE, X: values, Body: &ast.BlockStmt{List: []ast.Stmt{call}}}}
	}
	return []ast.Stmt{call}
}
