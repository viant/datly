package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (l *lowerer) keyFunction(name string, shape recordShape, key keyShape, parts []plan.KeyPart) *ast.FuncDecl {
	keyType := key.typeExpr()
	body := []ast.Stmt{&ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{
		Names: []*ast.Ident{ast.NewIdent("zero")}, Type: keyType,
	}}}}}
	body = append(body, &ast.IfStmt{
		Cond: &ast.BinaryExpr{X: ast.NewIdent("value"), Op: token.EQL, Y: ast.NewIdent("nil")},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("zero"), ast.NewIdent("false"))}},
	})
	for _, part := range parts {
		if !part.Type.Pointer && !strings.HasPrefix(strings.TrimSpace(part.Type.Name), "*") {
			continue
		}
		body = append(body, &ast.IfStmt{
			Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("value"), part.Field), Op: token.EQL, Y: ast.NewIdent("nil")},
			Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("zero"), ast.NewIdent("false"))}},
		})
	}
	body = append(body, returnStmt(keyValueExpr(key, parts, ast.NewIdent("value")), ast.NewIdent("true")))
	return &ast.FuncDecl{
		Name: ast.NewIdent(name),
		Type: &ast.FuncType{
			Params:  &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("value")}, Type: shape.pointerExpr()}}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: key.typeExpr()}, {Type: ast.NewIdent("bool")}}},
		},
		Body: &ast.BlockStmt{List: body},
	}
}

func (l *lowerer) currentIndexStatements(record *recordLowering) ([]ast.Stmt, error) {
	currentValues, err := pathExpr(record.plan.Current.InputPath)
	if err != nil {
		return nil, err
	}
	indexVariable := l.currentIndexVariable(record)
	currentVariable := fmt.Sprintf("current%d", record.order)
	currentOffset := fmt.Sprintf("currentIndex%d", record.order)
	keyVariable := fmt.Sprintf("currentKey%d", record.order)
	existsVariable := fmt.Sprintf("currentExists%d", record.order)
	mapType := &ast.MapType{Key: record.key.typeExpr(), Value: record.current.pointerExpr()}
	statements := []ast.Stmt{&ast.AssignStmt{
		Lhs: []ast.Expr{ast.NewIdent(indexVariable)}, Tok: token.DEFINE,
		Rhs: []ast.Expr{callExpr(ast.NewIdent("make"), mapType, callExpr(ast.NewIdent("len"), currentValues))},
	}}
	body := []ast.Stmt{}
	if !record.current.pointer {
		body = append(body, &ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent(currentVariable)}, Tok: token.DEFINE,
			Rhs: []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: &ast.IndexExpr{X: currentValues, Index: ast.NewIdent(currentOffset)}}},
		})
	}
	body = append(body,
		&ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent(keyVariable), ast.NewIdent("ok")}, Tok: token.DEFINE,
			Rhs: []ast.Expr{callExpr(ast.NewIdent(l.currentKeyFunction(record)), ast.NewIdent(currentVariable))},
		},
		&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("ok")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}},
		&ast.IfStmt{
			Init: &ast.AssignStmt{
				Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent(existsVariable)}, Tok: token.DEFINE,
				Rhs: []ast.Expr{&ast.IndexExpr{X: ast.NewIdent(indexVariable), Index: ast.NewIdent(keyVariable)}},
			},
			Cond: ast.NewIdent(existsVariable),
			Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(
				selectExpr(ast.NewIdent(l.fmtAlias), "Errorf"),
				stringExpr("duplicate current key for view %s"), stringExpr(record.plan.Current.ViewIdentity),
			))}},
		},
		&ast.AssignStmt{
			Lhs: []ast.Expr{&ast.IndexExpr{X: ast.NewIdent(indexVariable), Index: ast.NewIdent(keyVariable)}},
			Tok: token.ASSIGN, Rhs: []ast.Expr{ast.NewIdent(currentVariable)},
		},
	)
	rangeStatement := &ast.RangeStmt{Tok: token.DEFINE, X: currentValues, Body: &ast.BlockStmt{List: body}}
	if record.current.pointer {
		rangeStatement.Key = ast.NewIdent("_")
		rangeStatement.Value = ast.NewIdent(currentVariable)
	} else {
		rangeStatement.Key = ast.NewIdent(currentOffset)
	}
	return append(statements, rangeStatement), nil
}
