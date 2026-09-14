package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

type mutationEntityKey struct {
	entity string
	table  string
}

type validationBatch struct {
	key                 mutationEntityKey
	record              *recordLowering
	candidates, options string
}

func (role actionRole) entityKey() mutationEntityKey {
	// Canonical Data scopes support only the default connector; named connector
	// selection is rejected by Data.ValidationConnection.
	return mutationEntityKey{entity: role.record.value.base, table: role.record.plan.Table}
}

func (e *validationEmitter) batches() []validationBatch {
	var result []validationBatch
	seen := map[mutationEntityKey]int{}
	for _, role := range e.action.roles {
		key := role.entityKey()
		if _, ok := seen[key]; ok {
			continue
		}
		index := len(result)
		seen[key] = index
		name := "batch" + strconv.Itoa(index)
		result = append(result, validationBatch{
			key:        key,
			record:     role.record,
			candidates: name + "Candidates",
			options:    name + "Options",
		})
	}
	return result
}

func (e *validationEmitter) batchFor(role actionRole, batches []validationBatch) validationBatch {
	key := role.entityKey()
	for _, batch := range batches {
		if batch.key == key {
			return batch
		}
	}
	panic("missing generated validation batch for " + role.record.plan.Identity)
}

func (e *validationEmitter) declareBatch(batch validationBatch) []ast.Stmt {
	id := ast.NewIdent
	return []ast.Stmt{
		defineStmt(batch.candidates, &ast.CompositeLit{Type: &ast.ArrayType{Elt: batch.record.value.pointerExpr()}}),
		defineStmt(batch.options, &ast.CompositeLit{Type: &ast.ArrayType{Elt: selectExpr(id(e.action.l.handlerAlias), "ValidationOptions")}}),
	}
}

func (e *validationEmitter) validateBatch(batch validationBatch) []ast.Stmt {
	id := ast.NewIdent
	validation := id("validation")
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: &ast.BinaryExpr{X: callExpr(id("len"), id(batch.candidates)), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}},
			Body: &ast.BlockStmt{List: []ast.Stmt{
				&ast.AssignStmt{
					Lhs: []ast.Expr{validation, id("err")},
					Tok: token.DEFINE,
					Rhs: []ast.Expr{callExpr(selectExpr(e.member("Validator"), "Validate"), id("ctx"), id(batch.candidates), id(batch.options))},
				},
				errorGuard(id("err")),
				e.guard(&ast.BinaryExpr{X: validation, Op: token.EQL, Y: id("nil")}, "framework validator returned no result"),
				assignStmt(selectExpr(id("result"), "Failed"), &ast.BinaryExpr{X: selectExpr(id("result"), "Failed"), Op: token.LOR, Y: selectExpr(validation, "Failed")}),
				&ast.AssignStmt{
					Lhs: []ast.Expr{selectExpr(id("result"), "Violations")},
					Tok: token.ASSIGN,
					Rhs: []ast.Expr{&ast.CallExpr{Fun: id("append"), Args: []ast.Expr{selectExpr(id("result"), "Violations"), selectExpr(validation, "Violations")}, Ellipsis: 1}},
				},
			}},
		},
	}
}
