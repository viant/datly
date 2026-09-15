package golang

import (
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *actionEmitter) sequence() (ast.Decl, error) {
	body := []ast.Stmt{}
	if e.entities.identity != nil {
		body = append(body, errorGuard(callExpr(selectExpr(ast.NewIdent("frames"), "freezePendingIdentity"))))
	}
	// Validate all captured identities before invoking any allocator. Working
	// zero tests alone are never evidence that a client omitted an identity.
	for _, role := range e.roles {
		validated := selectExpr(ast.NewIdent("actions"), role.field+"Validated")
		body = append(body, assignStmt(validated, callExpr(ast.NewIdent("make"), &ast.MapType{Key: role.record.value.pointerExpr(), Value: role.record.value.pointerExpr()})), &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(role.field + "Options"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.entry + "PayloadOptions"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		sequence := role.record.plan.Sequence
		if sequence != nil {
			body = append(body, defineStmt(role.field+"Candidates", &ast.CompositeLit{Type: &ast.ArrayType{Elt: role.record.value.pointerExpr()}}))
		}
		loop := e.original(role)
		loop = append(loop, e.decisionIdentity(role, "_", "supplied")...)
		loop = append(loop, assignStmt(ast.NewIdent("_"), ast.NewIdent("supplied")))
		selection, action, err := e.actionSelection(role)
		if err != nil {
			return nil, err
		}
		loop = append(loop, selection...)
		loop = append(loop, assignStmt(ast.NewIdent("_"), action))
		loop = append(loop, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("baseline"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{e.runtimeCall("CloneValue", selectExpr(ast.NewIdent("frame"), "Entity"), ast.NewIdent(role.field+"Options"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, assignStmt(&ast.IndexExpr{X: validated, Index: selectExpr(ast.NewIdent("frame"), "Entity")}, &ast.TypeAssertExpr{X: ast.NewIdent("baseline"), Type: role.record.value.pointerExpr()}))
		if sequence != nil {
			appendCandidate := assignStmt(ast.NewIdent(role.field+"Candidates"), callExpr(ast.NewIdent("append"), ast.NewIdent(role.field+"Candidates"), selectExpr(ast.NewIdent("frame"), "Entity")))
			eligible := &ast.BinaryExpr{X: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("original"), "Has"), stringExpr(sequence.Field.Field))}}
			if e.entities.identity != nil && e.isIdentityField(role, sequence.Field.Field) {
				eligible = &ast.BinaryExpr{X: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(selectExpr(ast.NewIdent("frame"), "identityKnown"), sequence.Field.Field)}}
			}
			loop = append(loop, &ast.IfStmt{Cond: eligible, Body: &ast.BlockStmt{List: []ast.Stmt{appendCandidate}}})
		}
		body = append(body, e.frameLoop(role, loop))
	}
	reservations, err := e.sequenceReservations()
	if err != nil {
		return nil, err
	}
	body = append(body, reservations...)
	for _, role := range e.roles {
		sequence := role.record.plan.Sequence
		if sequence == nil {
			continue
		}
		candidates := ast.NewIdent(role.field + "Candidates")
		path, err := e.fieldPath(role, sequence.Field.Field)
		if err != nil {
			return nil, err
		}
		call := callExpr(selectExpr(selectExpr(ast.NewIdent("actions"), "Sequencer"), "Allocate"), ast.NewIdent("ctx"), stringExpr(role.record.plan.Table), candidates, stringExpr(strings.ReplaceAll(path, ".", "/")))
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(ast.NewIdent("len"), candidates), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(call)}}})
	}
	return e.phase("Sequence", 1, body), nil
}

// sequenceReservations exposes supplied and explicitly resolved IDs from every
// role in the allocation domain before the first Allocate call;
// detached payload clones keep optional custom reservation code away from them.
// Only explicitly selected deletes are excluded: their IDs already require
// matched Previous. Supplied insert/update IDs remain reserved even without
// local allocation candidates, because nested/custom work may allocate later.
func (e *actionEmitter) sequenceReservations() ([]ast.Stmt, error) {
	id := ast.NewIdent
	var body []ast.Stmt
	for _, role := range e.roles {
		for _, field := range e.sequenceFields(role) {
			path, err := e.fieldPath(role, field)
			if err != nil {
				return nil, err
			}
			block := []ast.Stmt{defineStmt("reserved", &ast.CompositeLit{Type: &ast.ArrayType{Elt: role.record.value.pointerExpr()}})}
			loop := e.original(role)
			if role.record.plan.Write.DeleteMarker.Field != "" {
				loop = append(loop, e.decisionIdentity(role, "_", "supplied")...)
				loop = append(loop, assignStmt(id("_"), id("supplied")))
				selection, action, err := e.actionSelection(role)
				if err != nil {
					return nil, err
				}
				loop = append(loop, selection...)
				loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(e.l.handlerAlias), "WriteDelete")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}})
			}
			clone := e.runtimeCall("CloneValue", selectExpr(id("original"), "original"), id(role.field+"Options"))
			capture := []ast.Stmt{
				&ast.AssignStmt{Lhs: []ast.Expr{id("cloned"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}},
				&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}},
				assignStmt(id("reserved"), callExpr(id("append"), id("reserved"), &ast.TypeAssertExpr{X: id("cloned"), Type: role.record.value.pointerExpr()})),
			}
			if e.entities.identity == nil || !e.isIdentityField(role, field) {
				loop = append(loop, &ast.IfStmt{Cond: callExpr(selectExpr(id("original"), "Has"), stringExpr(field)), Body: &ast.BlockStmt{List: capture}})
			}
			if e.entities.identity != nil && e.isIdentityField(role, field) {
				resolved := selectExpr(selectExpr(id("frame"), "identityKnown"), field)
				capture := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("clone"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{e.runtimeCall("CloneValue", selectExpr(id("frame"), "Entity"), id(role.field+"Options"))}}, errorGuard(id("err")), defineStmt("reservedRow", &ast.TypeAssertExpr{X: id("clone"), Type: role.record.value.pointerExpr()})}
				capture = append(capture, e.accessor(role.record, path, "reservedField")...)
				value := ast.Expr(selectExpr(selectExpr(id("frame"), "identityKey"), field))
				for _, key := range role.record.plan.IdentityKeys() {
					if key.Field == field && (key.Type.Pointer || strings.HasPrefix(key.Type.Name, "*")) {
						capture = append(capture, defineStmt("reservedValue", value))
						value = &ast.UnaryExpr{Op: token.AND, X: id("reservedValue")}
					}
				}
				capture = append(capture, errorGuard(e.runtimeCall("AssignFields", id("reservedRow"), e.assignment(id("reservedField"), value))), assignStmt(id("reserved"), callExpr(id("append"), id("reserved"), id("reservedRow"))))
				loop = append(loop, &ast.IfStmt{Cond: resolved, Body: &ast.BlockStmt{List: capture}})
			}
			block = append(block, e.frameLoop(role, loop))
			call := callExpr(selectExpr(id("reserver"), "Reserve"), id("ctx"), stringExpr(role.record.plan.Table), id("reserved"), stringExpr(strings.ReplaceAll(path, ".", "/")))
			block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(id("len"), id("reserved")), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(call)}}})
			body = append(body, &ast.BlockStmt{List: block})
		}
	}
	if len(body) == 0 {
		return nil, nil
	}
	contract := &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{id("Reserve")}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{Type: selectExpr(id(e.l.contextAlias), "Context")}, {Type: id("string")}, {Type: id("any")}, {Type: id("string")}}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("error")}}}}}}}}
	return []ast.Stmt{&ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{id("reserver"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.TypeAssertExpr{X: selectExpr(id("actions"), "Sequencer"), Type: contract}}}, Cond: id("ok"), Body: &ast.BlockStmt{List: body}}}, nil
}

func (e *actionEmitter) sequenceFields(role actionRole) []string {
	var result []string
	seen := map[string]bool{}
	for _, owner := range e.roles {
		sequence := owner.record.plan.Sequence
		if sequence == nil {
			continue
		}
		var field string
		if owner.record.value.base == role.record.value.base {
			// Register reused typed identities under each role's own table.
			// Only native metadata can decide whether spellings are aliases;
			// unrelated physical tables retain independent reservation sets.
			field = sequence.Field.Field
		} else if owner.record.plan.Table == role.record.plan.Table {
			// Distinct typed views can share a physical sequence key. Match its
			// canonical source, never a same-typed unrelated composite key part.
			keys := append([]plan.KeyPart(nil), role.record.plan.IdentityKeys()...)
			if own := role.record.plan.Sequence; own != nil {
				keys = append(keys, plan.KeyPart{Field: own.Field.Field, Source: own.Field.Source})
			}
			for _, key := range keys {
				if sequence.Field.Source != "" && key.Source == sequence.Field.Source {
					field = key.Field
					break
				}
			}
		}
		if field != "" && !seen[field] {
			seen[field] = true
			result = append(result, field)
		}
	}
	return result
}

func (e *actionEmitter) isIdentityField(role actionRole, name string) bool {
	for _, key := range role.record.plan.IdentityKeys() {
		if key.Field == name {
			return true
		}
	}
	return false
}
