package golang

import (
	"go/ast"
	"go/token"
	"path"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
	xhandler "github.com/viant/xdatly/handler"
)

// StatementContract uses the existing binder-aware generated handler product.
// All request state and capabilities are invocation-local; root completion is
// still performed by the canonical engine, after this contract only queues.
func StatementContract(value *plan.StatementPlan, config Config) (*ast.File, error) {
	file, err := StatementQueue(value, config)
	if err != nil {
		return nil, err
	}
	imports := file.Decls[0].(*ast.GenDecl)
	addImport := func(packagePath, preferred string) string {
		used := map[string]bool{}
		for _, item := range imports.Specs {
			spec := item.(*ast.ImportSpec)
			p, _ := strconv.Unquote(spec.Path.Value)
			alias := path.Base(p)
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if p == packagePath {
				return alias
			}
			used[alias] = true
		}
		alias := preferred
		for index := 2; used[alias]; index++ {
			alias = preferred + strconv.Itoa(index)
		}
		imports.Specs = append(imports.Specs, &ast.ImportSpec{Name: ast.NewIdent(alias), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(packagePath)}})
		return alias
	}
	contextAlias, handlerAlias := addImport("context", "context"), addImport(handlerPackage, "xhandler")
	handler := "_" + config.Factory + "Statement"
	contract := &ast.IndexListExpr{X: selectExpr(ast.NewIdent(handlerAlias), "Contract"), Indices: []ast.Expr{parseExpr(config.InputType), parseExpr(config.OutputType)}}
	file.Decls = append(file.Decls,
		&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(handler), Type: &ast.StructType{Fields: &ast.FieldList{}}}}},
		&ast.FuncDecl{Name: ast.NewIdent(config.Factory), Type: &ast.FuncType{Results: &ast.FieldList{List: []*ast.Field{{Type: contract}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(handler)}})}}},
	)
	dependencies := &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("DML")}, Type: selectExpr(ast.NewIdent(handlerAlias), "DML"), Tag: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.DMLKey))}}}}}
	body := []ast.Stmt{
		&ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("dependencies")}, Type: dependencies}}}},
		errorGuard(callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent("session"), "Binder")), "Bind"), ast.NewIdent("ctx"), &ast.UnaryExpr{Op: token.AND, X: ast.NewIdent("dependencies")})),
	}
	queueArgs := []ast.Expr{selectExpr(ast.NewIdent("dependencies"), "DML"), ast.NewIdent("input"), ast.NewIdent("output")}
	if len(value.SQLConstants) > 0 {
		constantAlias := addImport("github.com/viant/datly/constant", "constant")
		identifierAlias := addImport("github.com/viant/datly/constant/identifier", "identifier")
		entries := []ast.Expr{}
		for i, name := range value.SQLConstants {
			field := "SQLConst" + strconv.Itoa(i)
			dependencies.Fields.List = append(dependencies.Fields.List, &ast.Field{Names: []*ast.Ident{ast.NewIdent(field)}, Type: ast.NewIdent("string"), Tag: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote("parameter:\"" + field + ",kind=const,in=" + name + ",required=true\"")}})
			entries = append(entries, &ast.KeyValueExpr{Key: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(name)}, Value: selectExpr(ast.NewIdent("dependencies"), field)})
		}
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("constants"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent(constantAlias), "New"), &ast.CompositeLit{Type: &ast.MapType{Key: ast.NewIdent("string"), Value: ast.NewIdent("string")}, Elts: entries})}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		renderer := &ast.CompositeLit{Type: selectExpr(ast.NewIdent(identifierAlias), "Renderer"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Values"), Value: ast.NewIdent("constants")}}}
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("statementSQL"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(renderer, "Render"), &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(value.SQL)})}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		queueArgs = append(queueArgs, ast.NewIdent("statementSQL"))
	}
	body = append(body, returnStmt(callExpr(ast.NewIdent(config.Factory+"QueueStatement"), queueArgs...)))

	file.Decls = append(file.Decls, &ast.FuncDecl{Name: ast.NewIdent("Exec"), Recv: &ast.FieldList{List: []*ast.Field{namedField("h", &ast.StarExpr{X: ast.NewIdent(handler)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(contextAlias), "Context")), namedField("session", selectExpr(ast.NewIdent(handlerAlias), "Session")), namedField("input", &ast.StarExpr{X: parseExpr(config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(config.OutputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}})
	return file, nil
}
