package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

type entityAccessor struct{ name, path string }

func (e *entityEmitter) accessName(record *recordLowering, kind, name string) string {
	return "access" + strconv.Itoa(record.order) + kind + name
}
func (e *entityEmitter) accessDefinitions(record *recordLowering) ([]entityAccessor, error) {
	result := []entityAccessor{}
	if marker := record.plan.Entity.MarkerField; marker != "" {
		result = append(result, entityAccessor{e.accessName(record, "Marker", ""), marker})
		for _, field := range record.plan.Entity.Fields {
			result = append(result, entityAccessor{e.accessName(record, "Mark", field.Name), marker + "." + field.Name})
		}
	}
	for _, field := range record.plan.Entity.Fields {
		result = append(result, entityAccessor{e.accessName(record, "Value", field.Name), strings.Join(e.entityFieldPath(record, field.Name), ".")})
	}
	for _, key := range e.keys(record) {
		result = append(result, entityAccessor{e.accessName(record, "Key", key.Field), strings.Join(e.entityFieldPath(record, key.Field), ".")})
	}
	relations, err := e.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for _, relation := range relations {
		result = append(result, entityAccessor{e.accessName(record, "Relation", relation.storage), strings.Join(relation.holder, ".")})
	}
	return result, nil
}

func (e *entityEmitter) accessPlanFields() ([]*ast.Field, error) {
	result := []*ast.Field{namedField("options", selectExpr(ast.NewIdent(e.shapeAlias), "CloneOptions")), namedField("captureErr", ast.NewIdent("error"))}
	for _, record := range e.l.records {
		if !e.enabled(record) {
			continue
		}
		definitions, err := e.accessDefinitions(record)
		if err != nil {
			return nil, err
		}
		for _, definition := range definitions {
			result = append(result, namedField(definition.name, &ast.StarExpr{X: selectExpr(ast.NewIdent(e.shapeAlias), "Accessor")}))
		}
	}
	return result, nil
}

func (e *entityEmitter) accessPlanDeclaration() (ast.Decl, error) {
	owner := ast.NewIdent("owner")
	body := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("options"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(e.prefix + "CloneOptions"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, assignStmt(selectExpr(owner, "options"), ast.NewIdent("options"))}
	for _, record := range e.l.records {
		if !e.enabled(record) {
			continue
		}
		definitions, err := e.accessDefinitions(record)
		if err != nil {
			return nil, err
		}
		for _, definition := range definitions {
			typ := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.reflectAlias), "TypeOf"), callExpr(record.value.pointerExpr(), ast.NewIdent("nil"))), "Elem"))
			call := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.shapeAlias), "Linked"), typ), "Accessor"), stringExpr(definition.path))
			body = append(body, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("accessor"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}, Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}, Else: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(owner, definition.name), ast.NewIdent("accessor"))}}})
		}
	}
	body = append(body, returnStmt(ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent("prepare"), Recv: &ast.FieldList{List: []*ast.Field{namedField("owner", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}

func (e *entityEmitter) captureRead(accessor, target ast.Expr, name string, exit ast.Stmt) []ast.Stmt {
	return []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent(name + "Present"), ast.NewIdent(name + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(accessor, "GetOptional"), target)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(ast.NewIdent("s"), "captureErr"), ast.NewIdent(name+"Err")), exit}}}}
}
