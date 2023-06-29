package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

//go:embed tmpl/handler/index_by.gox
var indexTemplate string

//go:embed tmpl/handler/has_key.gox
var hasKeyTemplate string

type (
	IndexGenerator struct {
		state     reflect.Type
		builder   *strings.Builder
		index     receiverIndex
		stateName string

		exprToType           map[string]string
		variableToExpression map[string]string
		variableToType       map[string]reflect.Type
	}

	receiverIndex map[string]*ast.CallExpr

	CustomSlice struct {
		Name         string
		ReceiverName string
		ItemType     reflect.Type
		Item         string
	}

	IndexBy struct {
		FnName    string
		SliceType string
		IndexType string
	}
)

func NewIndexGenerator(stateType reflect.Type) *IndexGenerator {
	return &IndexGenerator{
		state:                stateType,
		builder:              &strings.Builder{},
		index:                receiverIndex{},
		stateName:            "state",
		exprToType:           map[string]string{},
		variableToExpression: map[string]string{},
		variableToType:       map[string]reflect.Type{},
	}
}

func (n *IndexGenerator) OnAssign(assign *ast.Assign) (ast.Expression, error) {
	ident, ok := assign.Holder.(*ast.Ident)
	if !ok {
		return assign, nil
	}

	stringify, err := n.stringify(assign.Expression)
	if err != nil {
		return nil, err
	}

	n.variableToExpression[ident.Name] = stringify
	return assign, nil
}
func (n *IndexGenerator) OnCallExpr(expr *ast.CallExpr) (ast.Expression, error) {
	switch expr.Name {
	case "IndexBy":
		return n.handleIndexBy(expr)
	case "HasKey":
		return n.handleHasKey(expr)
	default:
		return expr, nil
	}
}

func (n *IndexGenerator) handleIndexBy(expr *ast.CallExpr) (*ast.CallExpr, error) {
	receiver := expr.Receiver
	ident, ok := expr.Receiver.(*ast.Ident)
	if !ok || len(expr.Args) != 1 {
		return expr, nil
	}

	literalExpr, ok := expr.Args[0].(*ast.LiteralExpr)
	if !ok {
		return expr, nil
	}

	segments := strings.Split(ident.Name, ".")
	if !n.isStateParam(segments) {
		return expr, nil
	}

	structField, err := n.fieldByPath(segments)
	if err != nil {
		return nil, err
	}

	rawTypeName := structField.Tag.Get(xreflect.TagTypeName)
	typeName := rawTypeName
	if typeName == "" {
		typeName = segments[len(segments)-1]
	}

	receiverType := structField.Type
	receiverElem := n.deref(receiverType)
	var itemType reflect.Type
	if receiverElem.Kind() != reflect.Slice {
		receiver = n.slicifyHolder(ident, structField)
		itemType = receiverType
	} else {
		receiverElem = receiverElem.Elem()
		itemType = receiverElem
	}

	receiverElem = n.deref(receiverElem)
	if receiverElem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("can't IndexBy slice of non structs %v", receiverType.String())
	}

	fieldName := strings.Trim(literalExpr.Literal, `"`)
	indexByField, ok := receiverElem.FieldByName(fieldName)
	if !ok {
		return nil, n.fieldNotFoundError(fieldName, receiverType)
	}

	indexed, template := n.expandIndexByTemplate(xreflect.Stringify(n.deref(itemType), structField.Tag), indexByField.Type.String(), fieldName)
	n.appendFunction(template)

	stringify, err := n.stringify(expr)
	if err != nil {
		return nil, err
	}

	n.exprToType[stringify] = indexed.IndexType

	newExpr := *expr
	newExpr.Name = indexed.FnName
	newExpr.Receiver = ast.NewCallExpr(nil, indexed.SliceType, receiver)
	newExpr.Args = nil
	return &newExpr, nil
}

func (n *IndexGenerator) fieldByPath(segments []string) (reflect.StructField, error) {
	return n.pathInType(segments, n.state)
}

func (n *IndexGenerator) pathInType(segments []string, receiverType reflect.Type) (reflect.StructField, error) {
	var structField reflect.StructField
	for i := 0; i < len(segments); i++ {
		elem := n.deref(receiverType)
		if elem.Kind() != reflect.Struct {
			return reflect.StructField{}, fmt.Errorf("unsupported receiver type %v", receiverType.String())
		}

		field, ok := elem.FieldByName(segments[i])
		if !ok {
			return reflect.StructField{}, n.fieldNotFoundError(segments[i], receiverType)
		}

		receiverType = field.Type
		structField = field
	}
	return structField, nil
}

func (n *IndexGenerator) itemType(receiverType reflect.Type) reflect.Type {
	return n.deref(receiverType).Elem()
}

func (n *IndexGenerator) fieldNotFoundError(fieldName string, receiverType reflect.Type) error {
	return fmt.Errorf("not found field %v at struct %v", fieldName, receiverType.String())
}

func (n *IndexGenerator) deref(receiverType reflect.Type) reflect.Type {
	elem := receiverType
	for elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return elem
}

func (n *IndexGenerator) slicifyHolder(ident *ast.Ident, field reflect.StructField) ast.Expression {
	selector := ident.Name
	if ident.WithState {
		selector = n.stateName + "." + selector
	}

	return ast.NewLiteral(
		fmt.Sprintf("[]%v{ %v }", xreflect.Stringify(field.Type, field.Tag), selector),
	)
}

func (n *IndexGenerator) handleHasKey(expr *ast.CallExpr) (ast.Expression, error) {
	ident, ok := expr.Receiver.(*ast.Ident)
	if !ok || len(expr.Args) != 1 {
		return expr, nil
	}

	expression, ok := expr.Args[0].(*ast.Ident)
	if !ok {
		return expr, nil
	}

	rType, err := n.findVariableType(expression)
	if rType == nil || err != nil {
		return expr, err
	}

	variableToExpr := n.variableToExpression[ident.Name]
	receiverType, ok := n.exprToType[variableToExpr]
	if !ok {
		return expr, nil
	}

	template := n.expandHasKeyTemplate(receiverType, rType.String())
	n.appendFunction(template)
	return expr, nil
}

func (n *IndexGenerator) findVariableType(expression *ast.Ident) (reflect.Type, error) {
	split := strings.Split(expression.Name, ".")
	if !n.isStateParam(split) {
		rType, ok := n.variableToType[split[0]]
		if !ok {
			return nil, nil
		}

		inType, err := n.pathInType(split[1:], rType.Elem())
		if err != nil {
			return nil, err
		}

		return inType.Type, err
	}

	structField, err := n.fieldByPath(split)
	if err != nil {
		return nil, err
	}

	return structField.Type, nil
}

func (n *IndexGenerator) stringify(expression ast.Expression) (string, error) {
	builder := ast.NewBuilder(ast.Options{Lang: ast.LangGO})
	err := expression.Generate(builder)
	return builder.String(), err
}

func (n *IndexGenerator) isStateParam(split []string) bool {
	if _, ok := n.deref(n.state).FieldByName(split[0]); !ok {
		return false
	}

	return true
}

func (n *IndexGenerator) appendFunction(fnContent string) {
	n.builder.WriteString("\n")
	n.builder.WriteString(fnContent)
}

func (n *IndexGenerator) OnSliceItem(value *ast.Ident, set *ast.Ident) error {
	rType, err := n.findVariableType(set)
	if rType == nil || err != nil {
		return err
	}

	n.variableToType[value.Name] = rType
	return nil
}

func (n *IndexGenerator) expandIndexByTemplate(itemType string, keyType string, fieldName string) (*IndexBy, string) {
	result := strings.ReplaceAll(indexTemplate, "$ValueType", itemType)
	result = strings.ReplaceAll(result, "$KeyType", keyType)
	result = strings.ReplaceAll(result, "$IndexName", fieldName)
	return &IndexBy{
		FnName:    "IndexBy" + fieldName,
		SliceType: itemType + "Slice",
		IndexType: "Indexed" + itemType,
	}, result
}

func (n *IndexGenerator) expandHasKeyTemplate(receiver string, field string) string {
	result := strings.ReplaceAll(hasKeyTemplate, "$IndexType", receiver)
	result = strings.ReplaceAll(result, "$KeyType", field)
	return result
}

func (n *IndexGenerator) OnConditionStmt(value *ast.Condition) (ast.Expression, error) {
	ident, ok := value.If.(*ast.Ident)
	if !ok {
		return value, nil
	}

	variableType, err := n.findVariableType(ident)
	if err != nil || variableType == nil {
		return value, err
	}

	if variableType.Kind() == reflect.Ptr {
		newCondition := *value
		newCondition.If = ast.NewBinary(ident, "!=", &ast.LiteralExpr{Literal: "nil"})
		return &newCondition, nil
	}
	return value, nil
}
