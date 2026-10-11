package golang

import (
	"fmt"
	"go/ast"
	"go/token"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func statementMergeCall(value *plan.StatementPlan, l *lowerer, selector func(plan.StatementSelector) (ast.Expr, error)) ([]ast.Expr, ast.Expr, error) {
	var args []ast.Expr
	for i, field := range []plan.StatementSelector{value.MergeTable, value.MergeRows, value.MergeConfig, value.MergeResult} {
		arg, err := selector(field)
		if err != nil {
			return nil, nil, err
		}
		if i == 3 {
			if !field.Addressable {
				return nil, nil, fmt.Errorf("Merge result destination is not addressable")
			}
			arg = &ast.UnaryExpr{Op: token.AND, X: arg}
		}
		args = append(args, arg)
	}
	importAlias := func(pkg, preferred string) string {
		alias := l.importsByPath[pkg]
		if alias == "" {
			alias = l.availableAlias(preferred)
			l.pathsByAlias[alias] = pkg
			l.importsByPath[pkg] = alias
		}
		l.usedImports[pkg] = true
		return alias
	}
	info := importAlias("github.com/viant/sqlx/metadata/info", "sqlxinfo")
	options := importAlias("github.com/viant/sqlx/moption", "mergeoption")
	capability := &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("Merge")}, Type: &ast.FuncType{
		Params: &ast.FieldList{List: []*ast.Field{
			{Type: ast.NewIdent("string")}, {Type: ast.NewIdent("any")},
			{Type: selectExpr(ast.NewIdent(info), "MergeConfig")},
			{Type: &ast.StarExpr{X: selectExpr(ast.NewIdent(info), "MergeResult")}},
			{Type: &ast.Ellipsis{Elt: selectExpr(ast.NewIdent(options), "Option")}},
		}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
	}}}}}
	return args, capability, nil
}
