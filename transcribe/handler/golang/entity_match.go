package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func (e *entityEmitter) matchKeyName(record *recordLowering) string {
	return e.prefix + "MatchKey" + strconv.Itoa(record.order)
}
func (e *entityEmitter) matchAdapterName(record *recordLowering) string {
	return e.prefix + "MatchAdapter" + strconv.Itoa(record.order)
}
func (e *entityEmitter) currentIdentityName(record *recordLowering) string {
	return e.prefix + "CurrentIdentity" + strconv.Itoa(record.order)
}

func (e *entityEmitter) matchDeclarations(record *recordLowering) ([]ast.Decl, error) {
	fields := []*ast.Field{}
	for _, key := range e.keys(record) {
		typ, err := e.l.keyType(key.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, namedField(key.Field, parseExpr(typ)))
	}
	keyType := ast.NewIdent(e.matchKeyName(record))
	keyDeclaration := &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: keyType, Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}
	adapter := &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.matchAdapterName(record)), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("owner", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}}}}}
	current, err := e.matchCurrentKey(record)
	if err != nil {
		return nil, err
	}
	original := []ast.Decl{e.matchOriginalKey(record)}
	if e.identity != nil {
		original = e.identity.declarations(e, record)
	}
	result := []ast.Decl{keyDeclaration, adapter, e.matchSource(record)}
	result = append(result, original...)
	return append(result, current, e.matchEqual(record), e.currentIdentity(record)), nil
}

func (e *entityEmitter) currentIdentity(record *recordLowering) ast.Decl {
	state, current := ast.NewIdent("original"), ast.NewIdent("current")
	keyType := ast.NewIdent(e.matchKeyName(record))
	adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: ast.NewIdent(e.matchAdapterName(record)), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: selectExpr(state, "owner")}}}}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.CompositeLit{Type: keyType}, ast.NewIdent("false"), e.invariantError("captured identity accessors are required"))}}}, returnStmt(callExpr(selectExpr(adapter, "CurrentKey"), current))}
	return &ast.FuncDecl{Name: ast.NewIdent(e.currentIdentityName(record)), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("original", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: keyType}, {Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) adapterReceiver(record *recordLowering) *ast.FieldList {
	return &ast.FieldList{List: []*ast.Field{namedField("_", ast.NewIdent(e.matchAdapterName(record)))}}
}
func (e *entityEmitter) matchSource(record *recordLowering) ast.Decl {
	state := ast.NewIdent("state")
	return &ast.FuncDecl{Name: ast.NewIdent("Source"), Recv: e.adapterReceiver(record), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: record.value.pointerExpr()}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, returnStmt(selectExpr(state, "source"))}}}
}

func (e *entityEmitter) matchOriginalKey(record *recordLowering) ast.Decl {
	state := ast.NewIdent("state")
	keyType := ast.NewIdent(e.matchKeyName(record))
	zero := &ast.CompositeLit{Type: keyType}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil"))}}}}
	keys := e.keys(record)
	if len(keys) == 0 {
		body = append(body, returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil")))
	} else {
		body = append(body, defineStmt("supplied", &ast.BasicLit{Kind: token.INT, Value: "0"}))
		for _, key := range keys {
			body = append(body, &ast.IfStmt{Cond: callExpr(selectExpr(state, "Has"), stringExpr(key.Field)), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IncDecStmt{X: ast.NewIdent("supplied"), Tok: token.INC}}}})
		}
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("supplied"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil"))}}})
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("supplied"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(len(keys))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), e.invariantError(fmt.Sprintf("entity %s has a partial original identity", record.value.base)))}}})
		values := []ast.Expr{}
		for _, key := range keys {
			body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(state, "key"+key.Field+"Valid")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), e.invariantError("original identity field "+key.Field+" is supplied without a value"))}}})
			values = append(values, &ast.KeyValueExpr{Key: ast.NewIdent(key.Field), Value: selectExpr(state, "key"+key.Field)})
		}
		body = append(body, returnStmt(&ast.CompositeLit{Type: keyType, Elts: values}, ast.NewIdent("true"), ast.NewIdent("nil")))
	}
	return &ast.FuncDecl{Name: ast.NewIdent("Key"), Recv: e.adapterReceiver(record), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: keyType}, {Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) matchCurrentKey(record *recordLowering) (ast.Decl, error) {
	current := ast.NewIdent("current")
	keyType := ast.NewIdent(e.matchKeyName(record))
	zero := &ast.CompositeLit{Type: keyType}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: current, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil"))}}}}
	values := []ast.Expr{}
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("adapter"), "owner"), Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), e.invariantError("captured key accessors are required"))}}})
	for index, key := range e.keys(record) {
		name := "key" + strconv.Itoa(index)
		accessor := selectExpr(selectExpr(ast.NewIdent("adapter"), "owner"), e.accessName(record, "Key", key.Field))
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent(name + "Present"), ast.NewIdent(name + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(accessor, "GetOptional"), current)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent(name+"Err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent(name + "Present")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil"))}}})
		var value ast.Expr = ast.NewIdent(name)
		if key.Type.Pointer || strings.HasPrefix(strings.TrimSpace(key.Type.Name), "*") {
			body = append(body, &ast.IfStmt{Cond: callExpr(selectExpr(value, "IsNil")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, ast.NewIdent("false"), ast.NewIdent("nil"))}}})
			value = callExpr(selectExpr(value, "Elem"))
		}
		typ, err := e.l.keyType(key.Type)
		if err != nil {
			return nil, err
		}
		value = &ast.TypeAssertExpr{X: callExpr(selectExpr(value, "Interface")), Type: parseExpr(typ)}
		values = append(values, &ast.KeyValueExpr{Key: ast.NewIdent(key.Field), Value: value})
	}
	body = append(body, returnStmt(&ast.CompositeLit{Type: keyType, Elts: values}, ast.NewIdent(strconv.FormatBool(len(values) > 0)), ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent("CurrentKey"), Recv: &ast.FieldList{List: []*ast.Field{namedField("adapter", ast.NewIdent(e.matchAdapterName(record)))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr())}}, Results: &ast.FieldList{List: []*ast.Field{{Type: keyType}, {Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}

func (e *entityEmitter) matchEqual(record *recordLowering) ast.Decl {
	state, current := ast.NewIdent("state"), ast.NewIdent("current")
	guard := &ast.BinaryExpr{X: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(state, "original"), Op: token.EQL, Y: ast.NewIdent("nil")}}
	clone := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shapeAlias), "Runtime")}}, "CloneValue"), current, selectExpr(selectExpr(state, "owner"), "options"))
	body := []ast.Stmt{&ast.IfStmt{Cond: guard, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"), e.invariantError("original processing baseline is missing"))}}}, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"), ast.NewIdent("err"))}}}, returnStmt(callExpr(selectExpr(ast.NewIdent(e.reflectAlias), "DeepEqual"), ast.NewIdent("cloned"), selectExpr(state, "original")), ast.NewIdent("nil"))}
	return &ast.FuncDecl{Name: ast.NewIdent("Equal"), Recv: e.adapterReceiver(record), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("state", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
