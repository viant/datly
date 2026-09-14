package golang

import (
	"fmt"
	"go/ast"
)

func (e *entityEmitter) statesName(record *recordLowering) string {
	return fmt.Sprintf("states%d", record.order)
}

func (e *entityEmitter) markerAllocator() ast.Decl {
	return &ast.FuncDecl{Name: ast.NewIdent(e.prefix + "NewMarker"), Type: &ast.FuncType{TypeParams: &ast.FieldList{List: []*ast.Field{namedField("M", ast.NewIdent("any"))}}, Params: &ast.FieldList{List: []*ast.Field{namedField("_", &ast.StarExpr{X: ast.NewIdent("M")})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: &ast.StarExpr{X: ast.NewIdent("M")}}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(ast.NewIdent("new"), ast.NewIdent("M")))}}}
}
