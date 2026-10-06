package golang

import (
	"go/ast"
	"go/token"
	"strings"
)

// Collection association distinguishes a captured delete from a captured insert.
// The original physical key adapter still owns Previous lookup and all DML keys.
func (e *entityEmitter) actionMatchDeclarations(record *recordLowering) []ast.Decl {
	if record.plan.Write.ActionPolicy != "insert-delete" {
		return nil
	}
	id := ast.NewIdent
	keyName, adapterName := e.matchKeyName(record)+"Operation", e.matchAdapterName(record)+"Operation"
	physical := id(e.matchAdapterName(record))
	keyType := id(keyName)
	zero := &ast.CompositeLit{Type: keyType}
	marker := record.plan.Write.DeleteMarker.Field
	decls := []ast.Decl{
		&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: keyType, Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("Key", id(e.matchKeyName(record))), namedField("Delete", id("bool"))}}}}}},
		&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(adapterName), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{Type: physical}}}}}}},
	}
	method := func(name string, params, results []*ast.Field, body []ast.Stmt) ast.Decl {
		return &ast.FuncDecl{Name: id(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("adapter", id(adapterName))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: params}, Results: &ast.FieldList{List: results}}, Body: &ast.BlockStmt{List: body}}
	}
	guard := func(condition ast.Expr, values ...ast.Expr) ast.Stmt {
		return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(values...)}}}
	}
	nilTest := func(value ast.Expr) ast.Expr { return &ast.BinaryExpr{X: value, Op: token.EQL, Y: id("nil")} }
	read := func(access, source ast.Expr, name string, returns ...ast.Expr) []ast.Stmt {
		return []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id(name), id(name + "Present"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(access, "GetOptional"), source)}}, guard(&ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, returns...)}
	}
	body := []ast.Stmt{guard(&ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: id("supplied")}, Op: token.LOR, Y: nilTest(id("entity"))}, id("false"), id("nil"))}
	body = append(body, read(selectExpr(selectExpr(id("adapter"), "owner"), e.accessName(record, "Value", marker)), id("entity"), "value", id("false"), id("err"))...)
	body = append(body, guard(&ast.UnaryExpr{Op: token.NOT, X: id("valuePresent")}, id("false"), id("nil")))
	value := ast.Expr(id("value"))
	for _, field := range record.plan.Entity.Fields {
		if field.Name == marker && (field.Type.Pointer || strings.HasPrefix(field.Type.Name, "*")) {
			body = append(body, guard(callExpr(selectExpr(value, "IsNil")), id("false"), id("nil")))
			value = callExpr(selectExpr(value, "Elem"))
			break
		}
	}
	body = append(body, returnStmt(callExpr(selectExpr(value, "Bool")), id("nil")))
	decls = append(decls, method("deletePartition", []*ast.Field{namedField("entity", record.value.pointerExpr()), namedField("supplied", id("bool"))}, []*ast.Field{{Type: id("bool")}, {Type: id("error")}}, body))
	results := []*ast.Field{{Type: keyType}, {Type: id("bool")}, {Type: id("error")}}
	for _, current := range []bool{false, true} {
		name, param, typ := "Key", "state", e.statePointer(record)
		if current {
			name, param, typ = "CurrentKey", "entity", record.value.pointerExpr()
		}
		body = []ast.Stmt{guard(nilTest(id(param)), zero, id("false"), id("nil")), &ast.AssignStmt{Lhs: []ast.Expr{id("key"), id("assigned"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(id("adapter"), e.matchAdapterName(record)), name), id(param))}}, guard(&ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, zero, id("false"), id("err"))}
		var entity, supplied ast.Expr = selectExpr(id("state"), "original"), callExpr(selectExpr(id("state"), "Has"), stringExpr(marker))
		if current {
			entity = id("entity")
			body = append(body, read(selectExpr(selectExpr(id("adapter"), "owner"), e.accessName(record, "Mark", marker)), entity, "mark", zero, id("false"), id("err"))...)
			supplied = &ast.BinaryExpr{X: id("markPresent"), Op: token.LAND, Y: callExpr(selectExpr(id("mark"), "Bool"))}
		}
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("remove"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("adapter"), "deletePartition"), entity, supplied)}}, guard(&ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, zero, id("false"), id("err")), returnStmt(&ast.CompositeLit{Type: keyType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("Key"), Value: id("key")}, &ast.KeyValueExpr{Key: id("Delete"), Value: id("remove")}}}, id("assigned"), id("nil")))
		decls = append(decls, method(name, []*ast.Field{namedField(param, typ)}, results, body))
	}
	return decls
}
