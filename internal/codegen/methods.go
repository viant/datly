package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

//go:embed tmpl/fn_template.gox
var functionSchema string

type (
	MethodNotifier struct {
		state   reflect.Type
		builder *strings.Builder
		index   receiverIndex
	}

	receiverIndex map[string]*ast.CallExpr
)

func NewMethodNotifier(stateType reflect.Type) *MethodNotifier {
	return &MethodNotifier{
		state:   stateType,
		builder: &strings.Builder{},
		index:   receiverIndex{},
	}
}
func (n *MethodNotifier) OnCallExpr(expr *ast.CallExpr) (*ast.CallExpr, error) {
	if expr.Name != "IndexBy" {
		return expr, nil
	}

	ident, ok := expr.Receiver.(*ast.Ident)
	if !ok {
		return expr, nil
	}

	if len(expr.Args) != 1 {
		return expr, nil
	}

	literalExpr, ok := expr.Args[0].(*ast.LiteralExpr)
	if !ok {
		return expr, nil
	}

	segments := strings.Split(ident.Name, ".")

	receiverType := n.state
	var structField reflect.StructField
	for i := 0; i < len(segments); i++ {
		elem := n.deref(receiverType)
		if elem.Kind() != reflect.Struct {
			return nil, fmt.Errorf("unsupported receiver type %v", receiverType.String())
		}

		field, ok := elem.FieldByName(segments[i])
		if !ok {
			if i == 0 {
				return nil, nil
			}

			return nil, n.fieldNotFoundError(segments[i], receiverType)
		}

		receiverType = field.Type
		structField = field
	}

	receiverElem := n.deref(receiverType)
	if receiverElem.Kind() != reflect.Slice {
		return nil, fmt.Errorf("can't IndexBy non-slice type %v", receiverType.String())
	}

	receiverElem = n.deref(receiverElem.Elem())
	if receiverElem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("can't IndexBy slice of non structs %v", receiverType.String())
	}

	fieldName := strings.Trim(literalExpr.Literal, `"`)
	indexByField, ok := receiverElem.FieldByName(fieldName)
	if !ok {
		return nil, n.fieldNotFoundError(literalExpr.Literal, receiverType)
	}

	receiverName := string(ident.Name[0])
	if structField.Type.Kind() == reflect.Ptr {
		receiverName = "*" + receiverName
	}

	rawTypeName := structField.Tag.Get(xreflect.TagTypeName)
	typeName := rawTypeName
	if typeName == "" {
		typeName = segments[len(segments)-1]
	}

	newTypeName := typeName + "Slice"
	fnName := expr.Name + literalExpr.Literal
	resultType := fmt.Sprintf("map[%v]%v{}", indexByField.Type.String(), structField.Tag.Get(xreflect.TagTypeName))
	fnContent := ast.Block{
		&ast.Assign{
			Holder:     &ast.Ident{Name: "index"},
			Expression: &ast.LiteralExpr{Literal: resultType},
		},
		&ast.Foreach{
			Value: &ast.Ident{Name: "item"},
			Set:   &ast.Ident{Name: receiverName},
			Body: ast.Block{
				&ast.Assign{
					Holder: &ast.MapExpr{
						Map: &ast.Ident{Name: "index"},
						Key: &ast.Ident{Name: "item." + fieldName},
					},
					Expression: &ast.Ident{Name: "item"},
				},
			},
		},
	}

	builder := ast.NewBuilder(ast.Options{Lang: ast.LangGO})
	if err := fnContent.Generate(builder); err != nil {
		return nil, err
	}

	result := strings.Replace(functionSchema, "$receiver", fmt.Sprintf("(%v %)", receiverName, typeName), 1)
	result = strings.Replace(result, "$fnName", fnName, 1)
	result = strings.Replace(result, "$in", "", 1)
	result = strings.Replace(result, "$out", resultType, 1)
	result = strings.Replace(result, "$body", builder.String(), 1)

	n.builder.WriteString(fmt.Sprintf("\ntype %v []%v", newTypeName, typeName))
	n.builder.WriteString("\n")
	n.builder.WriteString(result)

	newExpr := *expr
	newExpr.Name = typeName
	newExpr.Receiver = ast.NewCallExpr(nil, newTypeName, expr.Receiver)
	return &newExpr, nil
}

func (n *MethodNotifier) itemType(receiverType reflect.Type) reflect.Type {
	return n.deref(receiverType).Elem()
}

func (n *MethodNotifier) fieldNotFoundError(fieldName string, receiverType reflect.Type) error {
	return fmt.Errorf("not found field %v at struct %v", fieldName, receiverType.String())
}

func (n *MethodNotifier) deref(receiverType reflect.Type) reflect.Type {
	elem := receiverType
	for elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return elem
}
