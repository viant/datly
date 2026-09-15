package golang

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *entityEmitter) parentLookupMethod(parent *recordLowering, relation *plan.RelationPlan) (ast.Decl, error) {
	id := ast.NewIdent
	lookup := relation.Child.Current.Lookup
	if parent.plan.Current == nil || len(lookup.Columns) != len(relation.Links) {
		return nil, fmt.Errorf("scoped child read requires parent projection")
	}
	var fields []*ast.Field
	var values []ast.Expr
	var nonnull ast.Expr
	for i, link := range relation.Links {
		var source *plan.FieldRef
		for _, field := range parent.plan.Current.Fields {
			if field.Entity.Field == link.Parent.Field {
				value := field.Current
				source = &value
				break
			}
		}
		if source == nil {
			return nil, fmt.Errorf("scoped child read parent field %s is not projected", link.Parent.Field)
		}
		typ, err := e.l.typeReference(source.Type)
		if err != nil {
			return nil, err
		}
		field := namedField(link.Child.Field, typ)
		field.Tag = stringExpr(`sqlx:"` + lookup.Columns[i] + `"`).(*ast.BasicLit)
		fields = append(fields, field)
		value := selectExpr(id("row"), source.Field)
		values = append(values, &ast.KeyValueExpr{Key: id(link.Child.Field), Value: value})
		shape, err := e.lookupType(source.Type)
		if err != nil {
			return nil, err
		}
		if shape.pointer {
			check := &ast.BinaryExpr{X: value, Op: token.EQL, Y: id("nil")}
			if nonnull == nil {
				nonnull = check
			} else {
				nonnull = &ast.BinaryExpr{X: nonnull, Op: token.LOR, Y: check}
			}
		}
	}
	tuple := &ast.StructType{Fields: &ast.FieldList{List: fields}}
	result := &ast.ArrayType{Elt: tuple}
	loop := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("row"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}}}
	if nonnull != nil {
		loop = append(loop, &ast.IfStmt{Cond: nonnull, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("nil"), e.invariantError("scoped parent identity is null"))}}})
	}
	loop = append(loop, assignStmt(id("result"), callExpr(id("append"), id("result"), &ast.CompositeLit{Type: tuple, Elts: values})))
	method := &ast.FuncDecl{Name: id(lookup.Name), Recv: &ast.FieldList{List: []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("previous", &ast.ArrayType{Elt: parent.current.pointerExpr()})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: result}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{defineStmt("result", &ast.CompositeLit{Type: result}), &ast.RangeStmt{Key: id("_"), Value: id("row"), Tok: token.DEFINE, X: id("previous"), Body: &ast.BlockStmt{List: loop}}, returnStmt(id("result"), id("nil"))}}}
	var signature bytes.Buffer
	if err := format.Node(&signature, token.NewFileSet(), method.Type); err != nil {
		return nil, err
	}
	e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: e.l.config.InputType, Name: lookup.Name, Signature: signature.String()})
	return method, nil
}
