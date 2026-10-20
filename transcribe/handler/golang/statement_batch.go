package golang

import (
	"fmt"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"go/ast"
	"go/token"
	"strconv"
)

func statementBatchCall(value *plan.StatementPlan, selector func(plan.StatementSelector) (ast.Expr, error)) ([]ast.Expr, ast.Expr, error) {
	args := make([]ast.Expr, 0, 4)
	for i, field := range []plan.StatementSelector{value.BatchTable, value.BatchRows, value.BatchSize, value.BatchAffected} {
		arg, err := selector(field)
		if err != nil {
			return nil, nil, err
		}
		if i == 3 {
			if !field.Addressable {
				return nil, nil, fmt.Errorf("batch result is not addressable")
			}
			arg = &ast.UnaryExpr{Op: token.AND, X: arg}
		}
		args = append(args, arg)
	}
	capability := &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent(value.Method + "WithBatchSize")}, Type: &ast.FuncType{
		Params:  &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("string")}, {Type: ast.NewIdent("any")}, {Type: ast.NewIdent("int")}, {Type: &ast.StarExpr{X: ast.NewIdent("int64")}}}},
		Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
	}}}}}
	if value.BatchOnDuplicateKeySQL != nil {
		args = append(args, &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(*value.BatchOnDuplicateKeySQL)})
		method := capability.Methods.List[0]
		method.Names[0].Name += "AndOnDuplicateKeySQL"
		method.Type.(*ast.FuncType).Params.List = append(method.Type.(*ast.FuncType).Params.List, &ast.Field{Type: ast.NewIdent("string")})
	}
	return args, capability, nil
}
