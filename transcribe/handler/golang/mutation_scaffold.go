package golang

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

// MutationScaffold is a target proposal over a detached semantic plan. Bindings
// identify only newly proposed hook types; authored hooks retain their authority.
type MutationScaffold struct {
	File     *ast.File
	Plan     *plan.Plan
	Bindings []MutationScaffoldBinding
}

type MutationScaffoldBinding struct {
	Identity, Hook, Entity, Parent string
	Path                           plan.FieldPath
	Root                           bool
}

// ScaffoldMutationHooks proposes invocation-scoped EntityHooks, never methods
// on the input/output contracts or foreign entity types. Self descendants use
// the same role's EntityState, including its typed SelfParent field.
func ScaffoldMutationHooks(value *plan.Plan, config Config) (*MutationScaffold, error) {
	if value == nil || config.PackagePath == "" {
		return nil, fmt.Errorf("mutation hook scaffolding requires a semantic plan and canonical package")
	}
	result := &MutationScaffold{Plan: value.Clone()}
	l := &lowerer{plan: result.Plan, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	file := &ast.File{Name: ast.NewIdent(l.config.Package)}
	parents := map[*plan.RecordPlan]*recordLowering{}
	for _, record := range l.records {
		for _, relation := range record.plan.Relations {
			if relation != nil {
				parents[relation.Child] = record
			}
		}
	}
	imports := map[string]string{}
	for alias, path := range l.pathsByAlias {
		imports[alias] = path
	}
	resolver := xshape.Resolver{Package: config.PackagePath, Imports: imports}
	for _, record := range l.records {
		if record.plan.Auxiliary {
			continue
		}
		if record.plan.Entity == nil {
			return nil, fmt.Errorf("mutation scaffold role %s has no canonical entity metadata", record.plan.Identity)
		}
		if !record.plan.Entity.Hooks.IsZero() {
			continue
		}
		identity := record.plan.Identity + "\x00" + strings.Join(record.plan.InputPath, "\x00")
		digest := sha256.Sum256([]byte(identity))
		name := strings.TrimPrefix(l.factory, "New") + fmt.Sprintf("EntityHooks_%x", digest[:6])
		if err := l.reserveDeclaration(name); err != nil {
			return nil, err
		}
		entity, err := resolver.Canonical(record.value.base)
		if err != nil {
			return nil, err
		}
		parent := "github.com/viant/xdatly/handler.NoParent"
		parentExpr := ast.Expr(selectExpr(ast.NewIdent(l.handlerAlias), "NoParent"))
		if enclosing := parents[record.plan]; enclosing != nil {
			parent, err = resolver.Canonical(enclosing.value.base)
			if err != nil {
				return nil, err
			}
			parentExpr = parseExpr(enclosing.value.base)
		}
		binding := MutationScaffoldBinding{Identity: record.plan.Identity, Path: append(plan.FieldPath(nil), record.plan.InputPath...), Hook: config.PackagePath + "." + name, Entity: entity, Parent: parent, Root: record.plan == result.Plan.Root}
		result.Bindings = append(result.Bindings, binding)
		record.plan.Entity.Hooks = spec.TypeRef{Package: config.PackagePath, Name: name}
		record.plan.Entity.HooksBind = false
		file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Doc: &ast.CommentGroup{List: []*ast.Comment{{Text: "// " + name + " customizes role " + strings.Join(record.plan.InputPath, ".") + "."}}}, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(name), Type: &ast.StructType{Fields: &ast.FieldList{}}}}})
		for _, method := range []string{"Init", "Validate", "AfterSequence", "AfterQueue"} {
			state := &ast.IndexListExpr{X: selectExpr(ast.NewIdent(l.handlerAlias), "EntityState"), Indices: []ast.Expr{parseExpr(record.value.base), parentExpr}}
			file.Decls = append(file.Decls, (&mutationScaffoldEmitter{name: name}).method(method, []*ast.Field{
				namedField("ctx", selectExpr(ast.NewIdent(l.contextAlias), "Context")), namedField("entity", &ast.StarExpr{X: parseExpr(record.value.base)}), namedField("state", state),
			}))
		}
		if binding.Root {
			file.Decls = append(file.Decls, (&mutationScaffoldEmitter{name: name}).method("Finalize", []*ast.Field{
				namedField("ctx", selectExpr(ast.NewIdent(l.contextAlias), "Context")), namedField("input", &ast.StarExpr{X: parseExpr(config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(config.OutputType)}), namedField("outcome", selectExpr(ast.NewIdent(l.handlerAlias), "Outcome")),
			}))
		}
	}
	if len(result.Bindings) != 0 {
		file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
		result.File = file
	}
	return result, nil
}

type mutationScaffoldEmitter struct {
	name string
}

func (e *mutationScaffoldEmitter) method(name string, parameters []*ast.Field) *ast.FuncDecl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("hooks", &ast.StarExpr{X: ast.NewIdent(e.name)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: parameters}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}
}
