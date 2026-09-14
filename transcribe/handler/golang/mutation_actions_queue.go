package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func (e *actionEmitter) payloadOptions(role actionRole) ast.Decl {
	typ := selectExpr(ast.NewIdent(e.shape), "CloneOptions")
	body := []ast.Stmt{defineStmt("options", &ast.CompositeLit{Type: typ})}
	reflectAlias := e.l.importsByPath["reflect"]
	if reflectAlias == "" {
		reflectAlias = e.l.availableAlias("reflect")
		e.l.pathsByAlias[reflectAlias] = "reflect"
		e.l.importsByPath["reflect"] = reflectAlias
	}
	rowType := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(reflectAlias), "TypeOf"), callExpr(role.record.value.pointerExpr(), ast.NewIdent("nil"))), "Elem"))
	paths := []ast.Expr{rowType}
	entity := &entityEmitter{l: e.l}
	for _, field := range role.record.plan.Entity.Fields {
		if field.Relation || (!field.Writable && !field.Identity) {
			continue
		}
		paths = append(paths, stringExpr(strings.Join(entity.entityFieldPath(role.record, field.Name), ".")))
		if marker := role.record.plan.Entity.MarkerField; marker != "" {
			paths = append(paths, stringExpr(marker+"."+field.Name))
		}
	}
	body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(ast.NewIdent("options"), "Select"), paths...)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.CompositeLit{Type: typ}, ast.NewIdent("err"))}}}, returnStmt(ast.NewIdent("options"), ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent(role.entry + "PayloadOptions"), Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: typ}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *actionEmitter) queue() (ast.Decl, error) {
	body, err := e.verifyBusiness()
	if err != nil {
		return nil, err
	}
	body = append(body, e.verifyOrder()...)
	for _, role := range e.roles {
		entries := selectExpr(ast.NewIdent("actions"), role.field)
		working := selectExpr(ast.NewIdent("actions"), role.field+"QueuedState")
		body = append(body, assignStmt(working, callExpr(ast.NewIdent("make"), &ast.MapType{Key: role.record.value.pointerExpr(), Value: role.record.value.pointerExpr()})))
		body = append(body, defineStmt(role.field+"Queued", callExpr(ast.NewIdent("make"), &ast.ArrayType{Elt: role.record.value.pointerExpr()}, callExpr(ast.NewIdent("len"), entries))))
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("options"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.entry + "PayloadOptions"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}
		loop := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: ast.NewIdent("entry"), Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Payload"), Op: token.EQL, Y: ast.NewIdent("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation payload was not reconciled"))}}}}
		current := selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "Entity")
		loop = append(loop, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("working"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{e.runtimeCall("CloneValue", current, ast.NewIdent("options"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, assignStmt(&ast.IndexExpr{X: working, Index: current}, &ast.TypeAssertExpr{X: ast.NewIdent("working"), Type: role.record.value.pointerExpr()}))
		clone := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "CloneValue"), selectExpr(ast.NewIdent("entry"), "Payload"), ast.NewIdent("options"))
		loop = append(loop, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		clearKeys := []ast.Expr{ast.NewIdent("cloned")}
		for _, key := range role.record.plan.IdentityKeys() {
			clearKeys = append(clearKeys, stringExpr(role.record.plan.Entity.MarkerField+"."+key.Field))
		}
		if len(clearKeys) > 1 {
			call := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "WithZeroFields"), clearKeys...)
			clear := []ast.Stmt{
				&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{call}},
				&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}},
			}
			loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteUpdate")}, Body: &ast.BlockStmt{List: clear}})
		}
		loop = append(loop, assignStmt(&ast.IndexExpr{X: ast.NewIdent(role.field + "Queued"), Index: ast.NewIdent("index")}, &ast.TypeAssertExpr{X: ast.NewIdent("cloned"), Type: role.record.value.pointerExpr()}))
		block = append(block, &ast.RangeStmt{Key: ast.NewIdent("index"), Value: ast.NewIdent("entry"), Tok: token.DEFINE, X: entries, Body: &ast.BlockStmt{List: loop}})
		body = append(body, &ast.BlockStmt{List: block})
	}
	cases := []ast.Stmt{}
	for _, role := range e.roles {
		payload := &ast.IndexExpr{X: ast.NewIdent(role.field + "Queued"), Index: selectExpr(ast.NewIdent("visit"), "Index")}
		entry := &ast.IndexExpr{X: selectExpr(ast.NewIdent("actions"), role.field), Index: selectExpr(ast.NewIdent("visit"), "Index")}
		operations := []ast.Stmt{}
		for _, action := range []struct{ name, method string }{{"WriteInsert", "Insert"}, {"WriteUpdate", "Update"}} {
			operations = append(operations, &ast.CaseClause{List: []ast.Expr{selectExpr(ast.NewIdent(e.l.handlerAlias), action.name)}, Body: []ast.Stmt{errorGuard(callExpr(selectExpr(selectExpr(ast.NewIdent("actions"), "DML"), action.method), stringExpr(role.record.plan.Table), payload))}})
		}
		operations = append(operations, &ast.CaseClause{Body: []ast.Stmt{returnStmt(e.errorExpr("mutation action changed after diff"))}})
		cases = append(cases, &ast.CaseClause{List: []ast.Expr{&ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(role.record.order)}}, Body: []ast.Stmt{&ast.SwitchStmt{Tag: selectExpr(entry, "Action"), Body: &ast.BlockStmt{List: operations}}}})
	}
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("visit"), Tok: token.DEFINE, X: selectExpr(ast.NewIdent("frames"), e.frames.OrderField), Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(selectExpr(ast.NewIdent("ctx"), "Err"))), &ast.SwitchStmt{Tag: selectExpr(ast.NewIdent("visit"), "Role"), Body: &ast.BlockStmt{List: cases}}}}})
	return e.phase("Queue", 4, body), nil
}
