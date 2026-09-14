package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func (e *entityEmitter) recordCaptureDeclaration(record *recordLowering) (ast.Decl, error) {
	entity, result, owner := ast.NewIdent("entity"), ast.NewIdent("result"), ast.NewIdent("s")
	cache := selectExpr(owner, e.recordsName(record))
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: entity, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(owner, "captureErr"), Op: token.NEQ, Y: ast.NewIdent("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, &ast.IfStmt{Init: defineStmt("found", &ast.IndexExpr{X: cache, Index: entity}), Cond: &ast.BinaryExpr{X: ast.NewIdent("found"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("found"))}}}, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.stateName(record)), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("source"), Value: entity}, &ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: owner}}}}), assignStmt(&ast.IndexExpr{X: cache, Index: entity}, result)}
	ordered := selectExpr(owner, e.statesName(record))
	body = append(body, assignStmt(ordered, callExpr(ast.NewIdent("append"), ordered, result)))
	if record.plan.Entity.MarkerField != "" {
		body = append(body, e.captureRead(selectExpr(owner, e.accessName(record, "Marker", "")), entity, "marker", returnStmt(ast.NewIdent("nil")))...)
		var present ast.Expr = &ast.BinaryExpr{X: ast.NewIdent("markerPresent"), Op: token.LAND, Y: callExpr(selectExpr(ast.NewIdent("marker"), "IsValid"))}
		if record.plan.Entity.MarkerPointer {
			present = &ast.BinaryExpr{X: present, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("marker"), "IsNil"))}}
		}
		statements := []ast.Stmt{assignStmt(selectExpr(result, "available"), ast.NewIdent("true"))}
		for index, field := range record.plan.Entity.Fields {
			name := "mark" + strconv.Itoa(index)
			statements = append(statements, e.captureRead(selectExpr(owner, e.accessName(record, "Mark", field.Name)), entity, name, returnStmt(ast.NewIdent("nil")))...)
			statements = append(statements, &ast.IfStmt{Cond: ast.NewIdent(name + "Present"), Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(selectExpr(result, "marker"), field.Name), callExpr(selectExpr(ast.NewIdent(name), "Bool")))}}})
		}
		body = append(body, &ast.IfStmt{Cond: present, Body: &ast.BlockStmt{List: statements}})
	}
	for index, key := range e.keys(record) {
		name := "key" + strconv.Itoa(index)
		body = append(body, e.captureRead(selectExpr(owner, e.accessName(record, "Key", key.Field)), entity, name, returnStmt(ast.NewIdent("nil")))...)
		var present ast.Expr = ast.NewIdent(name + "Present")
		var value ast.Expr = ast.NewIdent(name)
		if key.Type.Pointer || strings.HasPrefix(strings.TrimSpace(key.Type.Name), "*") {
			present = &ast.BinaryExpr{X: present, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(value, "IsNil"))}}
			value = callExpr(selectExpr(value, "Elem"))
		}
		typ, err := e.l.keyType(key.Type)
		if err != nil {
			return nil, err
		}
		body = append(body, &ast.IfStmt{Cond: present, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(result, "key"+key.Field), &ast.TypeAssertExpr{X: callExpr(selectExpr(value, "Interface")), Type: parseExpr(typ)}), assignStmt(selectExpr(result, "key"+key.Field+"Valid"), ast.NewIdent("true"))}}})
	}
	relations, err := e.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for index, relation := range relations {
		name := "relation" + strconv.Itoa(index)
		body = append(body, e.captureRead(selectExpr(owner, e.accessName(record, "Relation", relation.storage)), entity, name, returnStmt(ast.NewIdent("nil")))...)
		holder := e.reflectedRelationValue(ast.NewIdent(name), relation.child)
		capture := e.captureValues(holder, relation.child, selectExpr(result, relation.storage), owner)
		branch := &ast.IfStmt{Cond: ast.NewIdent(name + "Present"), Body: &ast.BlockStmt{List: capture}}
		if !relation.child.value.many {
			branch.Else = &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(result, relation.storage), &ast.CompositeLit{Type: e.stateSlice(relation.child), Elts: []ast.Expr{ast.NewIdent("nil")}})}}
		}
		body = append(body, branch)
	}
	body = append(body, returnStmt(result))
	return &ast.FuncDecl{Name: ast.NewIdent(e.captureName(record)), Recv: &ast.FieldList{List: []*ast.Field{namedField("s", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}}, Results: &ast.FieldList{List: []*ast.Field{{Type: e.statePointer(record)}}}}, Body: &ast.BlockStmt{List: body}}, nil
}

func (e *entityEmitter) reflectedRelationValue(value ast.Expr, record *recordLowering) ast.Expr {
	if !record.value.many && !record.value.pointer {
		return &ast.StarExpr{X: &ast.TypeAssertExpr{X: callExpr(selectExpr(callExpr(selectExpr(value, "Addr")), "Interface")), Type: record.value.pointerExpr()}}
	}
	return &ast.TypeAssertExpr{X: callExpr(selectExpr(value, "Interface")), Type: parseExpr(record.value.expression())}
}

func (e *entityEmitter) inputCaptureDeclaration() (ast.Decl, error) {
	input, result := ast.NewIdent("input"), ast.NewIdent("result")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: input, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("nil"))}}}}
	fields := []ast.Expr{}
	for _, record := range e.l.records {
		if e.enabled(record) {
			fields = append(fields, &ast.KeyValueExpr{Key: ast.NewIdent(e.recordsName(record)), Value: &ast.CompositeLit{Type: e.recordsType(record)}})
		}
	}
	body = append(body, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.snapshot), Elts: fields}}), &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(result, "prepare"))), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("err"))}}})
	values, err := pathExpr(e.l.plan.Root.InputPath)
	if err != nil {
		return nil, err
	}
	body = append(body, e.captureValues(values, e.l.recordByPlan[e.l.plan.Root], selectExpr(result, "Roots"), result)...)
	guard := &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(result, "captureErr"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), selectExpr(result, "captureErr"))}}}
	body = append(body, guard)
	baseline, err := e.captureSelectedBaseline(result, values)
	if err != nil {
		return nil, err
	}
	body = append(body, baseline...)
	if e.identity != nil {
		body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(result, "captureProducers"))), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("err"))}}})
	}
	body = append(body, guard, returnStmt(result, ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent(e.capture), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("any")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}

func (e *entityEmitter) captureValues(values ast.Expr, record *recordLowering, destination, receiver ast.Expr) []ast.Stmt {
	if record.value.many {
		initialize := &ast.IfStmt{Cond: &ast.BinaryExpr{X: values, Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(destination, callExpr(ast.NewIdent("make"), e.stateSlice(record), &ast.BasicLit{Kind: token.INT, Value: "0"}, callExpr(ast.NewIdent("len"), values)))}}}
		var item ast.Expr = &ast.IndexExpr{X: values, Index: ast.NewIdent("index")}
		if !record.value.pointer {
			item = &ast.UnaryExpr{Op: token.AND, X: item}
		}
		appendItem := assignStmt(destination, callExpr(ast.NewIdent("append"), destination, callExpr(selectExpr(receiver, e.captureName(record)), item)))
		result := []ast.Stmt{initialize, &ast.RangeStmt{Key: ast.NewIdent("index"), Tok: token.DEFINE, X: values, Body: &ast.BlockStmt{List: []ast.Stmt{appendItem}}}}
		if !record.value.pointer {
			state := ast.NewIdent("state")
			size := callExpr(ast.NewIdent("len"), values)
			result = append(result, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: state, Tok: token.DEFINE, X: destination, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: state, Op: token.NEQ, Y: ast.NewIdent("nil")}, Op: token.LAND, Y: &ast.BinaryExpr{X: selectExpr(state, "valueScopeSize"), Op: token.LSS, Y: size}}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(state, "valueScopeSize"), size)}}}}}})
		}
		return result
	}
	if !record.value.pointer {
		values = &ast.UnaryExpr{Op: token.AND, X: values}
	}
	return []ast.Stmt{assignStmt(destination, callExpr(ast.NewIdent("append"), destination, callExpr(selectExpr(receiver, e.captureName(record)), values)))}
}
