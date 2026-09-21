package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationInvariantAsset applies declared groups to the shared typed frames.
// It is a phase product, not a separate execution engine or complete Program.
type MutationInvariantAsset struct {
	File     *ast.File
	Function string
}

func MutationInvariantSupport(value *plan.Plan, config Config, entities *EntityAsset, layout *MutationFrameLayout) (*MutationInvariantAsset, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if entities == nil || layout == nil || !token.IsIdentifier(layout.TypeName) {
		return nil, fmt.Errorf("invariant phase requires entity support and shared frame layout")
	}
	e := &entityEmitter{l: l}
	l.pathsByAlias[l.fmtAlias] = "fmt"
	shapeAlias := l.availableAlias("xshape")
	l.pathsByAlias[shapeAlias] = "github.com/viant/x/shape"
	id := ast.NewIdent
	nilExpr := id("nil")
	errExpr := func(message string) ast.Expr {
		return callExpr(selectExpr(id(l.fmtAlias), "Errorf"), stringExpr(message))
	}
	checkContext := func() ast.Stmt {
		return &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(id("ctx"), "Err"))), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}}
	}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("ctx"), Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("frames"), Op: token.EQL, Y: nilExpr}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(errExpr("invariant phase requires context and frames"))}}}, checkContext()}
	groups := 0
	for _, record := range l.records {
		if record.plan.Entity == nil || len(record.plan.Entity.Invariants) == 0 {
			continue
		}
		role, err := layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		if role.EntityType != record.value.base || !token.IsIdentifier(role.Field) {
			return nil, fmt.Errorf("invariant frame type disagrees with entity %s", record.plan.Identity)
		}
		entity := selectExpr(id("frame"), "Entity")
		state := selectExpr(id("frame"), "State")
		previous := selectExpr(state, "Previous")
		loaded := selectExpr(state, "PreviousFields")
		loop := []ast.Stmt{checkContext(), &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("frame"), Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: entity, Op: token.EQL, Y: nilExpr}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(errExpr("invariant frame requires an entity"))}}}}
		unknown := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(id(shapeAlias), "Runtime")}}, "IsNil"), loaded)
		loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: previous, Op: token.NEQ, Y: nilExpr}, Op: token.LAND, Y: unknown}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(errExpr("invariant previous row requires actual loaded-field evidence"))}}})
		for _, group := range record.plan.Entity.Invariants {
			var helper string
			for _, candidate := range entities.Invariants {
				if candidate.Identity != record.plan.Identity || strings.Join(candidate.Path, ".") != strings.Join(record.plan.InputPath, ".") || candidate.Group != group.Name {
					continue
				}
				if helper != "" {
					return nil, fmt.Errorf("duplicate invariant helper for %s/%s", record.plan.Identity, group.Name)
				}
				helper = candidate.BackfillFunction
			}
			if !token.IsIdentifier(helper) {
				return nil, fmt.Errorf("invariant helper missing for %s/%s", record.plan.Identity, group.Name)
			}
			var invoke ast.Expr = callExpr(id(helper), entity, previous, loaded)
			if record.plan.Entity.Owned {
				invoke = callExpr(selectExpr(entity, "Backfill"+group.Name+"IfNeeded"), previous, loaded)
			}
			loop = append(loop, checkContext(), &ast.IfStmt{Init: defineStmt("err", invoke), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}})
			groups++
		}
		body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("frame"), Tok: token.DEFINE, X: selectExpr(id("frames"), role.Field), Body: &ast.BlockStmt{List: loop}})
	}
	if groups == 0 {
		return nil, nil
	}
	body = append(body, returnStmt(nilExpr))
	name := "_" + lowerInitial(l.factory) + "Invariants"
	if err := l.reserveDeclaration(name); err != nil {
		return nil, err
	}
	file := &ast.File{Name: id(config.Package), Decls: []ast.Decl{&ast.FuncDecl{Name: id(name), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("ctx", selectExpr(id(l.contextAlias), "Context")), namedField("frames", &ast.StarExpr{X: id(layout.TypeName)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}}}
	file.Decls = append([]ast.Decl{e.importDeclaration(file)}, file.Decls...)
	return &MutationInvariantAsset{File: file, Function: name}, nil
}
