package golang

import (
	"go/ast"
	"go/token"
)

func (e *programEmitter) capture() ast.Decl {
	id := ast.NewIdent
	nilExpr := id("nil")
	resultType := e.policyType("Program", parseExpr(e.l.config.OutputType))
	failure := func(condition ast.Expr, result ast.Expr, message string) ast.Stmt {
		return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(result, e.failure(message))}}}
	}
	body := []ast.Stmt{failure(&ast.BinaryExpr{X: id("p"), Op: token.EQL, Y: nilExpr}, nilExpr, "mutation definition is required")}
	values := []ast.Expr{&ast.KeyValueExpr{Key: id("input"), Value: id("input")}, &ast.KeyValueExpr{Key: id("output"), Value: callExpr(id("new"), parseExpr(e.l.config.OutputType))}, &ast.KeyValueExpr{Key: id("finalizer"), Value: selectExpr(id("p"), "Finalizer")}, &ast.KeyValueExpr{Key: id("actions"), Value: callExpr(id("new"), id(e.asset.Actions.TypeName))}, &ast.KeyValueExpr{Key: id("failed"), Value: id("true")}}
	values = append(values, &ast.KeyValueExpr{Key: id("validation"), Value: callExpr(id("new"), id(e.asset.Validation.TypeName))})
	if e.asset.Hooks != nil {
		values = append(values, &ast.KeyValueExpr{Key: id("hooks"), Value: callExpr(id("new"), id(e.asset.Hooks.TypeName))})
	}
	body = append(body, defineStmt("program", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: id(e.asset.Program), Elts: values}}), failure(&ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: nilExpr}, id("program"), "mutation input is required"), failure(e.isNil(id("ctx")), id("program"), "mutation capture context is required"))
	guard := func() ast.Stmt {
		return &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("program"), id("err"))}}}
	}
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("original"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(e.asset.Entities.CaptureFunction), id("ctx"), id("input"))}}, guard(), &ast.AssignStmt{Lhs: []ast.Expr{id("snapshot"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.TypeAssertExpr{X: id("original"), Type: &ast.StarExpr{X: id(e.asset.Entities.SnapshotType)}}}}, failure(&ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: id("ok")}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("snapshot"), Op: token.EQL, Y: nilExpr}}, id("program"), "mutation original snapshot is unavailable"), assignStmt(selectExpr(id("program"), "original"), id("snapshot")))
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("metadata"), id("_")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id(e.l.handlerAlias), "ReadMetadataFromContext"), id("ctx"))}}, &ast.AssignStmt{Lhs: []ast.Expr{id("database"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(e.asset.Frames.CaptureFunction), id("input"), id("metadata"))}}, guard(), &ast.AssignStmt{Lhs: []ast.Expr{id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(id("database"), "bindProducers"), id("snapshot"))}}, guard(), assignStmt(selectExpr(id("program"), "database"), id("database")), assignStmt(selectExpr(id("program"), "stage"), &ast.BasicLit{Kind: token.INT, Value: "1"}), assignStmt(selectExpr(id("program"), "failed"), id("false")), returnStmt(id("program"), nilExpr))
	if e.asset.Indexes != nil && e.asset.Indexes.CacheField != "" {
		preparation := []ast.Stmt{assignStmt(selectExpr(id("program"), "failed"), id("true")), assignStmt(id("err"), callExpr(selectExpr(id("input"), "PrepareReadIndexes"), id("ctx"))), guard(), assignStmt(selectExpr(id("program"), "failed"), id("false"))}
		body = append(body[:len(body)-1], append(preparation, body[len(body)-1])...)
	}
	if e.hasIdentityResolver() {
		resolver := selectExpr(id("p"), "ResolveIdentity")
		callback := []ast.Stmt{assignStmt(selectExpr(id("program"), "failed"), id("true")), &ast.AssignStmt{Lhs: []ast.Expr{id("indexes"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(e.asset.Indexes.BuildFunction), id("ctx"), id("input"))}}, guard(), assignStmt(id("err"), callExpr(resolver, id("ctx"), id("input"), id("indexes"))), guard(), assignStmt(selectExpr(id("program"), "failed"), id("false"))}
		statement := &ast.IfStmt{Cond: &ast.BinaryExpr{X: resolver, Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: callback}}
		body = append(body[:len(body)-1], statement, body[len(body)-1])
	}

	return e.method(e.asset.Definition, "Capture", []*ast.Field{e.contextParam(), namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}, []*ast.Field{{Type: resultType}, {Type: id("error")}}, body)
}

func (e *programEmitter) finalize(definition bool) ast.Decl {
	id := ast.NewIdent
	nilExpr := id("nil")
	receiver, name := e.asset.Program, "Finalize"
	params := []*ast.Field{e.contextParam()}
	input, output, finalizer := e.member("input"), e.member("output"), e.member("finalizer")
	if definition {
		receiver, name = e.asset.Definition, "FinalizeFailure"
		params = append(params, namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(e.l.config.OutputType)}))
		input, output, finalizer = id("input"), id("output"), selectExpr(id("p"), "Finalizer")
	}
	params = append(params, namedField("outcome", selectExpr(id(e.l.handlerAlias), "Outcome")))
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("p"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.failure("mutation finalization requires definition/program"))}}}}
	if !definition {
		body = append(body, &ast.IfStmt{Cond: e.member("finalized"), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.failure("mutation program was already finalized"))}}}, assignStmt(e.member("finalized"), id("true")))
	}
	body = append(body, &ast.IfStmt{Cond: e.isNil(id("ctx")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.failure("mutation finalization context is required"))}}})
	invoke := callExpr(selectExpr(finalizer, "Finalize"), id("ctx"), input, output, callExpr(selectExpr(id("outcome"), "Clone")))
	body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: e.isNil(finalizer)}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(invoke)}}})
	if !definition && e.asset.Hooks != nil {
		body = append(body, returnStmt(callExpr(selectExpr(e.member("hooks"), "Finalize"), id("ctx"), input, output, callExpr(selectExpr(id("outcome"), "Clone")))))
	} else {
		body = append(body, returnStmt(nilExpr))
	}
	return e.method(receiver, name, params, []*ast.Field{{Type: id("error")}}, body)
}

// Foreign input types cannot import their generated handler without a cycle.
// An application may supply this typed adapter when constructing the definition.
func (e *programEmitter) hasIdentityResolver() bool {
	return e.asset.Indexes != nil && e.l.config.ReadIndexes != nil && !e.l.config.ReadIndexes.Owned
}
func (e *programEmitter) definitionFields(finalizer ast.Expr) []*ast.Field {
	fields := []*ast.Field{namedField("Finalizer", finalizer)}
	if e.hasIdentityResolver() {
		fields = append(fields, namedField("ResolveIdentity", &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{e.contextParam(), namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("indexes", &ast.StarExpr{X: ast.NewIdent(e.asset.Indexes.TypeName)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}))
	}
	return fields
}
