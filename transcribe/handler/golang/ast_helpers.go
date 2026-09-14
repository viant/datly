package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func selectExpr(value ast.Expr, field string) ast.Expr {
	return &ast.SelectorExpr{X: value, Sel: ast.NewIdent(field)}
}

func callExpr(function ast.Expr, arguments ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{Fun: function, Args: arguments}
}

func stringExpr(value string) ast.Expr {
	return &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(value)}
}

func returnStmt(values ...ast.Expr) ast.Stmt {
	return &ast.ReturnStmt{Results: values}
}

func namedField(name string, typ ast.Expr) *ast.Field {
	return &ast.Field{Names: []*ast.Ident{ast.NewIdent(name)}, Type: typ}
}
func assignStmt(target, value ast.Expr) ast.Stmt {
	return &ast.AssignStmt{Lhs: []ast.Expr{target}, Tok: token.ASSIGN, Rhs: []ast.Expr{value}}
}
func defineStmt(name string, value ast.Expr) ast.Stmt {
	return &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name)}, Tok: token.DEFINE, Rhs: []ast.Expr{value}}
}

func errorGuard(call ast.Expr) ast.Stmt {
	return &ast.IfStmt{
		Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}},
		Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}},
	}
}

func pathExpr(path plan.FieldPath) (ast.Expr, error) {
	if len(path) == 0 {
		return nil, &pathError{message: "generated Go selector path is empty"}
	}
	root := strings.TrimSpace(path[0])
	var result ast.Expr
	switch root {
	case "Input":
		result = ast.NewIdent("input")
	case "Output":
		result = ast.NewIdent("output")
	default:
		return nil, &pathError{message: "generated Go selector path has unsupported root " + strconv.Quote(root)}
	}
	for _, field := range path[1:] {
		if !token.IsIdentifier(strings.TrimSpace(field)) || token.Lookup(strings.TrimSpace(field)).IsKeyword() {
			return nil, &pathError{message: "generated Go selector path contains an invalid field " + strconv.Quote(field)}
		}
		result = selectExpr(result, field)
	}
	return result, nil
}

func selectPathExpr(root ast.Expr, path plan.FieldPath) (ast.Expr, error) {
	if len(path) == 0 {
		return nil, &pathError{message: "generated Go relative selector path is empty"}
	}
	result := root
	for _, field := range path {
		if !token.IsIdentifier(strings.TrimSpace(field)) || token.Lookup(strings.TrimSpace(field)).IsKeyword() {
			return nil, &pathError{message: "generated Go relative selector path contains an invalid field " + strconv.Quote(field)}
		}
		result = selectExpr(result, field)
	}
	return result, nil
}

type pathError struct{ message string }

func (e *pathError) Error() string { return e.message }

func keyValueExpr(key keyShape, parts []plan.KeyPart, value ast.Expr) ast.Expr {
	if !key.compound {
		return keyFieldExpr(value, parts[0])
	}
	items := make([]ast.Expr, 0, len(parts))
	for _, part := range parts {
		items = append(items, &ast.KeyValueExpr{Key: ast.NewIdent(part.Field), Value: keyFieldExpr(value, part)})
	}
	return &ast.CompositeLit{Type: ast.NewIdent(key.name), Elts: items}
}

func keyFieldExpr(value ast.Expr, part plan.KeyPart) ast.Expr {
	field := selectExpr(value, part.Field)
	if part.Type.Pointer || strings.HasPrefix(strings.TrimSpace(part.Type.Name), "*") {
		return &ast.StarExpr{X: field}
	}
	return field
}
