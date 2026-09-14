package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// actionValidationEmitter keeps callback context and read evidence immutable.
// Persisted payload agreement has a separate, deliberately narrower projection.
type actionValidationEmitter struct{ action *actionEmitter }

func (e actionValidationEmitter) declarations() []ast.Decl {
	return []ast.Decl{e.contextOptions(), e.snapshot(false), e.snapshot(true), e.previous(false), e.previous(true), e.payload()}
}

func (e actionValidationEmitter) guard(stages ...int) []ast.Stmt {
	a, id := e.action, ast.NewIdent
	body := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("actions"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("validation action state is required"))}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("frames"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("validation frames are required"))}}},
		&ast.IfStmt{Cond: a.isNil(id("ctx")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("validation context is required"))}}},
		errorGuard(callExpr(selectExpr(id("ctx"), "Err"))),
		errorGuard(callExpr(selectExpr(id("frames"), a.verify), selectExpr(id("actions"), "Input"))),
	}
	stage := selectExpr(id("actions"), "stage")
	var invalid ast.Expr
	for _, allowed := range stages {
		other := &ast.BinaryExpr{X: stage, Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(allowed)}}
		if invalid == nil {
			invalid = other
		} else {
			invalid = &ast.BinaryExpr{X: invalid, Op: token.LAND, Y: other}
		}
	}
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(id("actions"), "failed"), Op: token.LOR, Y: invalid}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("validation action phase is unavailable"))}}})
	return body
}

func (e actionValidationEmitter) clone(name string, value ast.Expr) []ast.Stmt {
	id := ast.NewIdent
	return []ast.Stmt{
		&ast.AssignStmt{Lhs: []ast.Expr{id(name), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{e.action.runtimeCall("CloneValue", value, id("options"))}},
		errorGuard(id("err")),
	}
}

func (e actionValidationEmitter) options(function string) []ast.Stmt {
	id := ast.NewIdent
	return []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("options"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id(function))}}, errorGuard(id("err"))}
}

func (e actionValidationEmitter) contextOptions() ast.Decl {
	a, id := e.action, ast.NewIdent
	typ := selectExpr(id(a.shape), "CloneOptions")
	body := []ast.Stmt{defineStmt("options", &ast.CompositeLit{Type: typ})}
	inputType := callExpr(selectExpr(callExpr(selectExpr(id(a.l.importsByPath["reflect"]), "TypeOf"), callExpr(&ast.StarExpr{X: parseExpr(a.l.config.InputType)}, id("nil"))), "Elem"))
	selectInput := callExpr(selectExpr(id("options"), "Select"), inputType, stringExpr(strings.Join(a.l.plan.Root.InputPath[1:], ".")))
	body = append(body, &ast.IfStmt{Init: defineStmt("err", selectInput), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.CompositeLit{Type: typ}, id("err"))}}})
	entity := &entityEmitter{l: a.l}
	// Select every canonical context field, including read-only relations. The
	// same selection applies throughout the graph; unrelated service internals
	// on those entity types are neither cloned nor zeroed in the live context.
	for _, record := range a.l.records {
		if record.plan.Entity == nil {
			continue
		}
		rowType := callExpr(selectExpr(callExpr(selectExpr(id(a.l.importsByPath["reflect"]), "TypeOf"), callExpr(record.value.pointerExpr(), id("nil"))), "Elem"))
		args := []ast.Expr{rowType}
		for _, field := range record.plan.Entity.Fields {
			args = append(args, stringExpr(strings.Join(entity.entityFieldPath(record, field.Name), ".")))
			if marker := record.plan.Entity.MarkerField; marker != "" {
				args = append(args, stringExpr(marker+"."+field.Name))
			}
		}
		if record.plan.Current != nil {
			for _, field := range record.plan.Current.Fields {
				args = append(args, stringExpr(strings.Join(entity.entityFieldPath(record, field.Entity.Field), ".")))
			}
		}
		body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(id("options"), "Select"), args...)), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.CompositeLit{Type: typ}, id("err"))}}})
	}
	body = append(body, returnStmt(id("options"), id("nil")))
	return &ast.FuncDecl{Name: id(a.name + "ValidationContextOptions"), Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: typ}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e actionValidationEmitter) equal(left, right ast.Expr, message string) ast.Stmt {
	a, id := e.action, ast.NewIdent
	equal := callExpr(selectExpr(id(a.l.importsByPath["reflect"]), "DeepEqual"), left, right)
	return &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: equal}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr(message))}}}
}

func (e actionValidationEmitter) snapshot(verify bool) ast.Decl {
	a, id := e.action, ast.NewIdent
	body := e.guard(1, 4)
	body = append(body, errorGuard(callExpr(selectExpr(id("actions"), "VerifyPrevious"), id("ctx"), id("frames"))))
	body = append(body, e.options(a.name+"ValidationContextOptions")...)
	body = append(body, e.clone("projection", selectExpr(id("actions"), "Input"))...)
	baseline := selectExpr(id("actions"), "validationState")
	if verify {
		body = append(body, e.equal(baseline, id("projection"), "validator changed validation context (written field or marker, logical field or relationship)"))
	} else {
		body = append(body, assignStmt(baseline, &ast.TypeAssertExpr{X: id("projection"), Type: &ast.StarExpr{X: parseExpr(a.l.config.InputType)}}))
	}
	name := "CaptureValidation"
	if verify {
		name = "VerifyValidation"
	}
	return e.method(name, body)
}

func (e actionValidationEmitter) previous(verify bool) ast.Decl {
	a, id := e.action, ast.NewIdent
	body := e.guard(1)
	if verify {
		body = e.guard(1, 2, 3, 4, 5)
	}
	captured := selectExpr(id("actions"), "previousCaptured")
	invalid := ast.Expr(captured)
	if verify {
		invalid = &ast.UnaryExpr{Op: token.NOT, X: captured}
	}
	body = append(body, &ast.IfStmt{Cond: invalid, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("Previous evidence must be captured exactly once before callbacks"))}}})
	body = append(body, e.options(a.name+"ValidationContextOptions")...)
	for _, role := range a.roles {
		states := selectExpr(id("actions"), role.field+"PreviousState")
		evidence := selectExpr(id("actions"), role.field+"PreviousFields")
		framePointer := &ast.StarExpr{X: id(role.frame.FrameType)}
		maskType := &ast.MapType{Key: id("string"), Value: id("bool")}
		if !verify {
			body = append(body, assignStmt(states, callExpr(id("make"), &ast.MapType{Key: framePointer, Value: role.record.value.pointerExpr()})), assignStmt(evidence, callExpr(id("make"), &ast.MapType{Key: framePointer, Value: maskType})))
		}
		state := selectExpr(id("frame"), "State")
		loop := e.clone("previous", selectExpr(state, "Previous"))
		loop = append(loop, &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{id("fields")}, Type: maskType}}}})
		read := selectExpr(state, "PreviousFields")
		mask := []ast.Stmt{assignStmt(id("fields"), callExpr(id("make"), maskType))}
		seen := map[string]bool{}
		names := []string{}
		for _, field := range role.record.plan.Entity.Fields {
			names = append(names, field.Name)
		}
		if role.record.plan.Current != nil {
			for _, field := range role.record.plan.Current.Fields {
				names = append(names, field.Entity.Field)
			}
		}
		for _, name := range names {
			if seen[name] {
				continue
			}
			seen[name] = true
			mask = append(mask, assignStmt(&ast.IndexExpr{X: id("fields"), Index: stringExpr(name)}, callExpr(selectExpr(read, "Has"), stringExpr(name))))
		}
		loop = append(loop, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: a.isNil(read)}, Body: &ast.BlockStmt{List: mask}})
		baseline := &ast.IndexExpr{X: states, Index: id("frame")}
		priorFields := &ast.IndexExpr{X: evidence, Index: id("frame")}
		if verify {
			loop = append(loop,
				&ast.AssignStmt{Lhs: []ast.Expr{id("baseline"), id("found")}, Tok: token.DEFINE, Rhs: []ast.Expr{baseline}},
				&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: id("found")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(a.errorExpr("Previous frame was not captured"))}}},
				e.equal(id("baseline"), id("previous"), "callback changed immutable Previous contents"),
				e.equal(priorFields, id("fields"), "callback changed immutable Previous read evidence"),
			)
		} else {
			loop = append(loop, assignStmt(baseline, &ast.TypeAssertExpr{X: id("previous"), Type: role.record.value.pointerExpr()}), assignStmt(priorFields, id("fields")))
		}
		body = append(body, a.frameLoop(role, loop))
	}
	name := "VerifyPrevious"
	if !verify {
		name = "CapturePrevious"
		body = append(body, assignStmt(captured, id("true")))
	}
	return e.method(name, body)
}

func (e actionValidationEmitter) payload() ast.Decl {
	a, id := e.action, ast.NewIdent
	body := e.guard(4)
	for _, role := range a.roles {
		block := e.options(role.entry + "PayloadOptions")
		current := selectExpr(selectExpr(id("entry"), "Frame"), "Entity")
		loop := e.clone("working", current)
		loop = append(loop, e.clone("payload", selectExpr(id("entry"), "Payload"))...)
		loop = append(loop, e.equal(id("working"), id("payload"), "validation context does not match the reconciled written payload"))
		block = append(block, a.decisionLoop(role, loop))
		body = append(body, &ast.BlockStmt{List: block})
	}
	return e.method("VerifyPayload", body)
}

func (e actionValidationEmitter) method(name string, body []ast.Stmt) ast.Decl {
	a, id := e.action, ast.NewIdent
	body = append(body, returnStmt(id("nil")))
	return a.method(name, []*ast.Field{namedField("ctx", selectExpr(id(a.l.contextAlias), "Context")), namedField("frames", &ast.StarExpr{X: id(a.frames.TypeName)})}, []*ast.Field{{Type: id("error")}}, body)
}
