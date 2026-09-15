package golang

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

// lookupDeclarations emits typed, invocation-local indexes using the same
// canonical record/current fields as mutation snapshots. It does not read a DB,
// mutate presence, cache invocation data, or bind additional request state.
func (e *entityEmitter) lookupDeclarations() ([]ast.Decl, error) {
	var result []ast.Decl
	for _, parent := range e.l.records {
		for _, relation := range parent.plan.Relations {
			if relation.Child == nil || relation.Child.Current == nil || relation.Child.Current.Lookup == nil {
				continue
			}
			var method ast.Decl
			var err error
			if relation.Child.Current.Lookup.ParentOnly {
				method, err = e.parentLookupMethod(parent, relation)
			} else {
				method, err = e.lookupMethod(parent, relation)
			}
			if err != nil {
				return nil, err
			}
			result = append(result, method)
		}
	}
	return result, nil
}

type lookupField struct {
	base    string
	pointer bool
}

func (e *entityEmitter) lookupType(ref spec.TypeRef) (lookupField, error) {
	expression, err := e.l.typeReference(ref)
	if err != nil {
		return lookupField{}, err
	}
	var out bytes.Buffer
	if err = format.Node(&out, token.NewFileSet(), expression); err != nil {
		return lookupField{}, err
	}
	text := out.String()
	parsed, err := (xshape.Resolver{}).Reference(text)
	if err != nil {
		return lookupField{}, err
	}
	if len(parsed.Wrappers) > 1 || len(parsed.Wrappers) == 1 && parsed.Wrappers[0].Kind != xshape.WrapperPointer {
		return lookupField{}, fmt.Errorf("lookup field must be a comparable scalar: %s", text)
	}
	return lookupField{base: parsed.QualifiedName(), pointer: len(parsed.Wrappers) == 1}, nil
}
func (f lookupField) value(selector string) string {
	if f.pointer {
		return "*" + selector
	}
	return selector
}
func (f lookupField) nonnull(selector string) string {
	if f.pointer {
		return selector + " != nil"
	}
	return "true"
}

func (e *entityEmitter) lookupMethod(parent *recordLowering, relation *plan.RelationPlan) (ast.Decl, error) {
	lookup := relation.Child.Current.Lookup
	failure, err := renderExpr(e.invariantError("lookup current identity is duplicated"))
	if err != nil {
		return nil, err
	}
	if parent.plan.Current == nil || parent.plan.Entity == nil || len(lookup.Columns) != len(relation.Links) {
		return nil, fmt.Errorf("lookup %s requires canonical parent/current/link authority", lookup.Name)
	}
	root := e.l.recordByPlan[e.l.plan.Root]
	fields := map[string]plan.CurrentField{}
	for _, field := range parent.plan.Current.Fields {
		fields[field.Entity.Field] = field
	}
	var keyFields, currentKey, requestKey, currentNonNull, requestComplete []string
	for i, key := range parent.plan.IdentityKeys() {
		field, ok := fields[key.Field]
		if !ok {
			return nil, fmt.Errorf("lookup %s is missing current identity %s", lookup.Name, key.Field)
		}
		entity, err := e.lookupType(field.Entity.Type)
		if err != nil {
			return nil, err
		}
		current, err := e.lookupType(field.Current.Type)
		if err != nil {
			return nil, err
		}
		if entity.base != current.base {
			return nil, fmt.Errorf("lookup %s has incompatible identity types", lookup.Name)
		}
		keyFields = append(keyFields, fmt.Sprintf("K%d %s", i, entity.base))
		currentKey = append(currentKey, current.value("previous."+field.Current.Field))
		requestKey = append(requestKey, entity.value("row."+field.Entity.Field))
		currentNonNull = append(currentNonNull, current.nonnull("previous."+field.Current.Field))
		requestComplete = append(requestComplete, "row."+parent.plan.Entity.MarkerField+"."+field.Entity.Field, entity.nonnull("row."+field.Entity.Field))
	}
	if len(keyFields) == 0 {
		return nil, fmt.Errorf("lookup %s requires full parent identity", lookup.Name)
	}
	markerPresent := "true"
	if parent.plan.Entity.MarkerPointer {
		markerPresent = "row." + parent.plan.Entity.MarkerField + " != nil"
	}
	var resultFields, tupleFields, valueCode, resultValues, tupleValues []string
	for i, link := range relation.Links {
		field, ok := fields[link.Parent.Field]
		if !ok {
			return nil, fmt.Errorf("lookup %s lacks current parent field %s", lookup.Name, link.Parent.Field)
		}
		entity, err := e.lookupType(field.Entity.Type)
		if err != nil {
			return nil, err
		}
		current, err := e.lookupType(field.Current.Type)
		if err != nil {
			return nil, err
		}
		if entity.base != current.base {
			return nil, fmt.Errorf("lookup %s has incompatible FK types", lookup.Name)
		}
		value := fmt.Sprintf("value%d", i)
		// Project scalar values, never pointer identity or Has into the SQL tuple.
		resultFields = append(resultFields, fmt.Sprintf("Value%d %s %s", i, entity.base, strconv.Quote(`sqlx:`+strconv.Quote(lookup.Columns[i]))))
		tupleFields = append(tupleFields, fmt.Sprintf("Value%d %s", i, entity.base))
		code := fmt.Sprintf("var %s %s\nif %s && row.%s.%s {if !(%s){continue};%s=%s} else {if previous==nil || !(%s){continue};%s=%s}\n", value, entity.base, markerPresent, parent.plan.Entity.MarkerField, field.Entity.Field, entity.nonnull("row."+field.Entity.Field), value, entity.value("row."+field.Entity.Field), current.nonnull("previous."+field.Current.Field), value, current.value("previous."+field.Current.Field))
		valueCode = append(valueCode, code)
		resultValues = append(resultValues, value)
		tupleValues = append(tupleValues, value)
	}
	resultType := "[]struct{" + strings.Join(resultFields, ";") + "}"
	// The method is generated on the input shape and relocates with that shape.
	var body bytes.Buffer
	fmt.Fprintf(&body, "package %s\nfunc(input *%s) %s(body %s, current %s)(%s,error){\ntype identity struct{%s}\ntype lookupKey struct{%s}\n", e.l.config.Package, e.l.config.InputType, lookup.Name, root.value.expression(), parent.current.expression(), resultType, strings.Join(keyFields, ";"), strings.Join(tupleFields, ";"))
	fmt.Fprintf(&body, "index:=make(map[identity]*%s)\npreviousRows,err:=(%s.Collection[%s]{}).Pointers(current);if err!=nil{return nil,err}\nfor _,previous:=range previousRows{if previous==nil||!(%s){continue};key:=identity{%s};if _,exists:=index[key];exists{return nil,%s};index[key]=previous}\n", parent.current.base, e.shapeAlias, parent.current.base, strings.Join(currentNonNull, " && "), strings.Join(currentKey, ","), failure)
	fmt.Fprintf(&body, "seen:=make(map[lookupKey]bool)\nvar result %s\nrows,err:=(%s.Collection[%s]{}).Pointers(body);if err!=nil{return nil,err}\nfor _,row:=range rows{if row==nil{continue}\n", resultType, e.shapeAlias, root.value.base)
	depth, err := e.lookupPath(&body, root, parent)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(&body, "var previous *%s\nif %s && %s {previous=index[identity{%s}]}\n", parent.current.base, markerPresent, strings.Join(requestComplete, " && "), strings.Join(requestKey, ","))
	for _, code := range valueCode {
		body.WriteString(code)
	}
	fmt.Fprintf(&body, "tuple:=lookupKey{%s};if seen[tuple]{continue};seen[tuple]=true\nresult=append(result,struct{%s}{%s})\n", strings.Join(tupleValues, ","), strings.Join(resultFields, ";"), strings.Join(resultValues, ","))
	for i := 0; i <= depth; i++ {
		body.WriteString("}\n")
	}
	body.WriteString("return result,nil\n}\n")
	file, err := parser.ParseFile(token.NewFileSet(), "lookup.go", body.String(), parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("emit lookup %s: %w", lookup.Name, err)
	}
	method := file.Decls[0].(*ast.FuncDecl)
	var signature bytes.Buffer
	if err = format.Node(&signature, token.NewFileSet(), method.Type); err != nil {
		return nil, err
	}
	e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: e.l.config.InputType, Name: lookup.Name, Signature: signature.String()})
	return method, nil
}

func (e *entityEmitter) lookupPath(body *bytes.Buffer, root, parent *recordLowering) (int, error) {
	current := root
	depth := 0
	for current != parent {
		var next *plan.RelationPlan
		for _, relation := range current.plan.Relations {
			if len(relation.Child.InputPath) <= len(parent.plan.InputPath) && strings.Join(relation.Child.InputPath, ".") == strings.Join(parent.plan.InputPath[:len(relation.Child.InputPath)], ".") {
				next = relation
				break
			}
		}
		if next == nil {
			return 0, fmt.Errorf("lookup parent path is not canonical")
		}
		child := e.l.recordByPlan[next.Child]
		fmt.Fprintf(body, "children%d,err:=(%s.Collection[%s]{}).Pointers(row.%s);if err!=nil{return nil,err}\nfor _,row:=range children%d{if row==nil{continue}\n", depth, e.shapeAlias, child.value.base, strings.Join(next.FieldPath, "."), depth)
		current = child
		depth++
	}
	return depth, nil
}
