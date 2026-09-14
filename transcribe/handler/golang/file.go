package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
	xhandler "github.com/viant/xdatly/handler"
)

func (l *lowerer) file() (*ast.File, error) {
	declarations := []ast.Decl{l.importDeclaration(), l.dependenciesDeclaration()}
	for _, hook := range l.relationHooks {
		declarations = append(declarations, l.relationHookDeclaration(hook))
	}
	if l.plan.Operation == plan.OperationPatch {
		for _, record := range l.records {
			if record.owner != record || record.plan.Current == nil {
				continue
			}
			if keyDeclaration := record.key.declaration(); keyDeclaration != nil {
				declarations = append(declarations, keyDeclaration)
			}
		}
	}
	declarations = append(declarations,
		l.handlerDeclaration(),
		l.contractAssertion(),
		l.factoryDeclaration(),
	)
	if l.plan.Operation == plan.OperationPatch {
		for _, record := range l.records {
			if record.owner != record || record.plan.Current == nil {
				continue
			}
			declarations = append(declarations,
				l.keyFunction(l.recordKeyFunction(record), record.value, record.key, record.plan.Keys),
				l.keyFunction(l.currentKeyFunction(record), record.current, record.key, record.plan.Current.Keys),
			)
		}
	}
	exec, err := l.execDeclaration()
	if err != nil {
		return nil, err
	}
	declarations = append(declarations, exec)
	return &ast.File{Name: ast.NewIdent(l.config.Package), Decls: declarations}, nil
}

func (l *lowerer) relationHookDeclaration(hook *relationHookLowering) ast.Decl {
	method := &ast.Field{
		Names: []*ast.Ident{ast.NewIdent(hook.methodName)},
		Type: &ast.FuncType{
			Params: &ast.FieldList{List: []*ast.Field{
				{Type: selectExpr(ast.NewIdent(l.contextAlias), "Context")},
				{Type: parseExpr(hook.holderType)},
			}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
		},
	}
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{
		Name: ast.NewIdent(hook.interfaceName),
		Type: &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{method}}},
	}}}
}

func (l *lowerer) importDeclaration() ast.Decl {
	type importItem struct{ alias, path string }
	items := []importItem{{alias: l.contextAlias, path: "context"}, {alias: l.handlerAlias, path: handlerPackage}}
	if l.usesFmt() {
		items = append(items, importItem{alias: l.fmtAlias, path: "fmt"})
	}
	for packagePath := range l.usedImports {
		items = append(items, importItem{alias: l.importsByPath[packagePath], path: packagePath})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].path < items[j].path })
	specs := make([]ast.Spec, 0, len(items))
	for _, item := range items {
		specs = append(specs, &ast.ImportSpec{
			Name: ast.NewIdent(item.alias), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(item.path)},
		})
	}
	return &ast.GenDecl{Tok: token.IMPORT, Lparen: 1, Specs: specs}
}

func (l *lowerer) dependenciesDeclaration() ast.Decl {
	fields := []*ast.Field{{
		Names: []*ast.Ident{ast.NewIdent("DML")},
		Type:  selectExpr(ast.NewIdent(l.handlerAlias), "DML"),
		Tag:   &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.DMLKey))},
	}}
	if l.plan.Root.Auxiliary {
		fields = nil
	}
	if l.usesSequencer() {
		fields = append(fields, &ast.Field{
			Names: []*ast.Ident{ast.NewIdent("Sequencer")},
			Type:  selectExpr(ast.NewIdent(l.handlerAlias), "Sequencer"),
			Tag:   &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.SequencerKey))},
		})
	}
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{
		Name: ast.NewIdent(l.dependencies), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}},
	}}}
}

func (l *lowerer) handlerDeclaration() ast.Decl {
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{
		Name: ast.NewIdent(l.handler), Type: &ast.StructType{Fields: &ast.FieldList{}},
	}}}
}

func (l *lowerer) contractType() ast.Expr {
	return &ast.IndexListExpr{
		X:       selectExpr(ast.NewIdent(l.handlerAlias), "Contract"),
		Indices: []ast.Expr{parseExpr(l.config.InputType), parseExpr(l.config.OutputType)},
	}
}

func (l *lowerer) contractAssertion() ast.Decl {
	return &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{
		Names:  []*ast.Ident{ast.NewIdent("_")},
		Type:   l.contractType(),
		Values: []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(l.handler)}}},
	}}}
}

func (l *lowerer) factoryDeclaration() ast.Decl {
	return &ast.FuncDecl{
		Name: ast.NewIdent(l.factory),
		Type: &ast.FuncType{Results: &ast.FieldList{List: []*ast.Field{{Type: l.contractType()}}}},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(
			&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(l.handler)}},
		)}},
	}
}

func (l *lowerer) execDeclaration() (*ast.FuncDecl, error) {
	inputType, err := parser.ParseExpr(strings.TrimSpace(l.config.InputType))
	if err != nil {
		return nil, err
	}
	outputType, err := parser.ParseExpr(strings.TrimSpace(l.config.OutputType))
	if err != nil {
		return nil, err
	}
	body := []ast.Stmt{&ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{
		Names: []*ast.Ident{ast.NewIdent("dependencies")}, Type: ast.NewIdent(l.dependencies),
	}}}}}
	body = append(body, errorGuard(callExpr(
		selectExpr(callExpr(selectExpr(ast.NewIdent("session"), "Binder")), "Bind"),
		ast.NewIdent("ctx"), &ast.UnaryExpr{Op: token.AND, X: ast.NewIdent("dependencies")},
	)))
	for _, record := range l.records {
		if record.plan.Sequence == nil {
			continue
		}
		destination, pathErr := pathExpr(record.plan.Sequence.Destination)
		if pathErr != nil {
			return nil, pathErr
		}
		selector := strings.Join(record.plan.Sequence.Selector, "/")
		if selector == "" {
			return nil, fmt.Errorf("generated Go sequence selector is empty for record %q", record.plan.Identity)
		}
		body = append(body, errorGuard(callExpr(
			selectExpr(selectExpr(ast.NewIdent("dependencies"), "Sequencer"), "Allocate"),
			ast.NewIdent("ctx"), stringExpr(record.plan.Table), destination, stringExpr(selector),
		)))
	}
	if l.plan.Operation == plan.OperationPatch {
		for _, record := range l.records {
			if record.owner != record || record.plan.Current == nil {
				continue
			}
			indexes, indexErr := l.currentIndexStatements(record)
			if indexErr != nil {
				return nil, indexErr
			}
			body = append(body, indexes...)
		}
	}
	writes, err := l.traversalStatements(l.records[0], nil)
	if err != nil {
		return nil, err
	}
	body = append(body, writes...)
	if l.plan.Output != nil {
		target, pathErr := pathExpr(l.plan.Output.Path)
		if pathErr != nil {
			return nil, pathErr
		}
		source, pathErr := pathExpr(l.plan.Root.InputPath)
		if pathErr != nil {
			return nil, pathErr
		}
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{target}, Tok: token.ASSIGN, Rhs: []ast.Expr{source}})
	}
	body = append(body, returnStmt(ast.NewIdent("nil")))
	return &ast.FuncDecl{
		Recv: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("h")}, Type: &ast.StarExpr{X: ast.NewIdent(l.handler)}}}},
		Name: ast.NewIdent("Exec"),
		Type: &ast.FuncType{
			Params: &ast.FieldList{List: []*ast.Field{
				{Names: []*ast.Ident{ast.NewIdent("ctx")}, Type: selectExpr(ast.NewIdent(l.contextAlias), "Context")},
				{Names: []*ast.Ident{ast.NewIdent("session")}, Type: selectExpr(ast.NewIdent(l.handlerAlias), "Session")},
				{Names: []*ast.Ident{ast.NewIdent("input")}, Type: &ast.StarExpr{X: inputType}},
				{Names: []*ast.Ident{ast.NewIdent("output")}, Type: &ast.StarExpr{X: outputType}},
			}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
		},
		Body: &ast.BlockStmt{List: body},
	}, nil
}
