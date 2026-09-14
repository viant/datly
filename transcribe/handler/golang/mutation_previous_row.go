package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *previousEmitter) visitError() ast.Stmt {
	return &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}
}
func (e *previousEmitter) visit(record *recordLowering, association EntityAssociation, role MutationPreviousRole, keys []plan.KeyPart) ([]ast.Stmt, error) {
	nilExpr, row := ast.NewIdent("nil"), ast.NewIdent("row")
	visited := &ast.IndexExpr{X: ast.NewIdent("visited"), Index: row}
	active := &ast.IndexExpr{X: ast.NewIdent("active"), Index: row}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: row, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}}, &ast.IfStmt{Cond: active, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}}}
	fieldsCall := callExpr(selectExpr(ast.NewIdent("projection"), "Fields"), ast.NewIdent("ordinal"), ast.NewIdent("steps"))
	fieldsCall.Ellipsis = 1
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("fields"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{fieldsCall}}, e.visitError(), &ast.IfStmt{Cond: e.isNil(ast.NewIdent("fields")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("actual row field provenance is required"))}}})
	repeated := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(ast.NewIdent("len"), ast.NewIdent("steps")), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("duplicate database root row"))}}}}
	names := map[string]bool{}
	for _, key := range record.plan.Current.Keys {
		names[key.Field] = true
	}
	for _, field := range record.plan.Current.Fields {
		names[field.Current.Field] = true
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		repeated = append(repeated, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(ast.NewIdent("priorFields"), "Has"), stringExpr(name)), Op: token.NEQ, Y: callExpr(selectExpr(ast.NewIdent("fields"), "Has"), stringExpr(name))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("shared database row has conflicting read evidence for " + name))}}})
	}
	repeated = append(repeated, returnStmt(nilExpr))
	body = append(body, &ast.IfStmt{Init: defineStmt("priorFields", visited), Cond: &ast.BinaryExpr{X: ast.NewIdent("priorFields"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: repeated}}, assignStmt(visited, ast.NewIdent("fields")), assignStmt(active, ast.NewIdent("true")), &ast.DeferStmt{Call: callExpr(ast.NewIdent("delete"), ast.NewIdent("active"), row)})
	keyValues := []ast.Expr{}
	for i, key := range record.plan.Current.Keys {
		name := "key" + strconv.Itoa(i)
		body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("fields"), "Has"), stringExpr(key.Field))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("database identity field " + key.Field + " was not loaded"))}}})
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent("present"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("keyAccess"+strconv.Itoa(i)), "GetOptional"), row)}}, e.visitError(), &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("present")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("database identity field " + key.Field + " is absent"))}}})
		var value ast.Expr = ast.NewIdent(name)
		if key.Type.Pointer || strings.HasPrefix(key.Type.Name, "*") {
			body = append(body, &ast.IfStmt{Cond: callExpr(selectExpr(value, "IsNil")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("database identity field " + key.Field + " is null"))}}})
			value = callExpr(selectExpr(value, "Elem"))
		}
		typ, err := e.l.keyType(keys[i].Type)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, &ast.KeyValueExpr{Key: ast.NewIdent(keys[i].Field), Value: &ast.TypeAssertExpr{X: callExpr(selectExpr(value, "Interface")), Type: parseExpr(typ)}})
	}
	body = append(body, defineStmt("key", &ast.CompositeLit{Type: ast.NewIdent(association.KeyType), Elts: keyValues}), &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("exists")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.IndexExpr{X: selectExpr(ast.NewIdent("result"), "byKey"), Index: ast.NewIdent("key")}}}, Cond: ast.NewIdent("exists"), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("duplicate database identity in " + record.plan.Current.ViewIdentity))}}}, defineStmt("previous", callExpr(ast.NewIdent("new"), parseExpr(record.value.base))))
	for i, field := range record.plan.Current.Fields {
		if field.Conversion != plan.LinkDirect && field.Conversion != plan.LinkAddress && field.Conversion != plan.LinkDereference {
			return nil, fmt.Errorf("current projection %s has no checked conversion", field.Entity.Field)
		}
		sourceType, err := e.l.typeReference(field.Current.Type)
		if err != nil {
			return nil, err
		}
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("source"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("sourceAccess"+strconv.Itoa(i)), "Get"), row)}}, e.visitError(), defineStmt("value", &ast.TypeAssertExpr{X: callExpr(selectExpr(ast.NewIdent("source"), "Interface")), Type: sourceType})}
		var value ast.Expr = ast.NewIdent("value")
		switch field.Conversion {
		case plan.LinkAddress:
			value = &ast.UnaryExpr{Op: token.AND, X: value}
		case plan.LinkDereference:
			block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: value, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("loaded null cannot project into scalar field " + field.Entity.Field))}}})
			value = &ast.StarExpr{X: value}
		}
		block = append(block, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(ast.NewIdent("targetAccess"+strconv.Itoa(i)), "Set"), ast.NewIdent("previous"), value)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		body = append(body, &ast.IfStmt{Cond: callExpr(selectExpr(ast.NewIdent("fields"), "Has"), stringExpr(field.Current.Field)), Body: &ast.BlockStmt{List: block}})
	}
	clone := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "CloneValue"), ast.NewIdent("previous"), ast.NewIdent("options"))
	mappedFields := &ast.CompositeLit{Type: ast.NewIdent(role.TypeName + "Fields"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("source"), Value: ast.NewIdent("fields")}}}
	entry := &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(role.EntryType), Elts: []ast.Expr{
		&ast.KeyValueExpr{Key: ast.NewIdent("value"), Value: &ast.TypeAssertExpr{X: ast.NewIdent("detached"), Type: record.value.pointerExpr()}},
		&ast.KeyValueExpr{Key: ast.NewIdent("fields"), Value: mappedFields},
	}}}
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("detached"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, e.visitError(), assignStmt(&ast.IndexExpr{X: selectExpr(ast.NewIdent("result"), "byKey"), Index: ast.NewIdent("key")}, entry))
	for i, holder := range record.plan.Current.Self {
		collection := &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: selectExpr(ast.NewIdent(e.shape), "Collection"), Index: parseExpr(record.current.base)}}}
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("value"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("selfAccess"+strconv.Itoa(i)), "Get"), row)}}, e.visitError(), &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("children"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(collection, "Pointers"), callExpr(selectExpr(ast.NewIdent("value"), "Interface")))}}, e.visitError()}
		stepType := selectExpr(ast.NewIdent(e.l.handlerAlias), "ReadStep")
		copySteps := callExpr(ast.NewIdent("append"), callExpr(&ast.ArrayType{Elt: stepType}, nilExpr), ast.NewIdent("steps"))
		copySteps.Ellipsis = 1
		steps := callExpr(ast.NewIdent("append"), copySteps, &ast.CompositeLit{Type: stepType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Holder"), Value: stringExpr(holder.Field)}, &ast.KeyValueExpr{Key: ast.NewIdent("Index"), Value: ast.NewIdent("index")}}})
		block = append(block, &ast.RangeStmt{Key: ast.NewIdent("index"), Value: ast.NewIdent("child"), Tok: token.DEFINE, X: ast.NewIdent("children"), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", callExpr(ast.NewIdent("visit"), ast.NewIdent("child"), ast.NewIdent("ordinal"), steps)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}}})
		body = append(body, &ast.BlockStmt{List: block})
	}
	body = append(body, returnStmt(nilExpr))
	return body, nil
}
