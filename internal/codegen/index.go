package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
	"reflect"
	"strings"
)

//go:embed tmpl/handler/index_by.gox
var indexTemplate string

//go:embed tmpl/handler/has_key.gox
var hasKeyTemplate string

type (
	IndexGenerator struct {
		state        State
		paramsByName map[string]*Parameter
		builder      *strings.Builder
		index        receiverIndex
		stateName    string

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

func NewIndexGenerator(specState State) *IndexGenerator {
	return &IndexGenerator{
		state:                specState,
		paramsByName:         specState.IndexByName(),
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

func (n *IndexGenerator) handleIndexBy(expr *ast.CallExpr) (ast.Expression, error) {
	receiver := expr.Receiver
	ident, ok := expr.Receiver.(*ast.Ident)
	if !ok || len(expr.Args) != 1 {
		return expr, nil
	}

	literal, ok := expr.Args[0].(*ast.LiteralExpr)
	if !ok {
		return expr, nil
	}

	segments := strings.Split(ident.Name, ".")
	if !n.isStateParam(segments[0]) {
		return expr, nil
	}

	stateParam, err := n.lookupParam(segments[0])
	if err != nil {
		return nil, err
	}

	literalValue := strings.Trim(literal.Literal, `"`)
	structQLParamName := ident.Name + literalValue

	structQLParam, err := n.lookupParam(structQLParamName)
	if err != nil {
		return expr, nil
	}

	if structQLParam.In.Kind != view.KindParam {
		return expr, nil
	}

	receiverType := stateParam.Schema.Type()
	receiverElem := n.deref(receiverType)
	fmt.Printf("cardinality  :%v\n", stateParam.Schema.Cardinality)
	if receiverElem.Kind() != reflect.Slice { //Cardinality
		receiver = n.slicifyHolder(stateParam)
	} else {
		receiverElem = receiverElem.Elem()
	}

	indexed, template := n.expandIndexByTemplate(stateParam, structQLParam)
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

func (n *IndexGenerator) lookupParam(name string) (*Parameter, error) {
	param, ok := n.paramsByName[name]
	if !ok {
		return nil, fmt.Errorf("failed to lookup state param: %v", name)
	}
	return param, nil
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

func (n *IndexGenerator) slicifyHolder(param *Parameter) ast.Expression {
	return ast.NewLiteral(
		fmt.Sprintf("[]%v{ %v }", param.Schema.DataType, param.LocalVariable()),
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
	if !n.isStateParam(split[0]) {

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

	param, err := n.lookupParam(split[0])
	if err != nil {
		return nil, err
	}

	return param.Schema.Type(), nil
}

func (n *IndexGenerator) stringify(expression ast.Expression) (string, error) {
	builder := ast.NewBuilder(ast.Options{Lang: ast.LangGO})
	err := expression.Generate(builder)
	return builder.String(), err
}

func (n *IndexGenerator) isStateParam(name string) bool {
	_, ok := n.paramsByName[name]
	return ok
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

func (n *IndexGenerator) expandIndexByTemplate(param *Parameter, qlParam *Parameter) (*IndexBy, string) {
	result := strings.ReplaceAll(indexTemplate, "$ValueType", param.Schema.DataType)
	result = strings.ReplaceAll(result, "$KeyType", qlParam.IndexField.Schema.DataType)
	result = strings.ReplaceAll(result, "$IndexName", qlParam.IndexField.Name)
	return &IndexBy{
		FnName:    "IndexBy" + qlParam.IndexField.Name,
		SliceType: param.Schema.DataType + "Slice",
		IndexType: "Indexed" + param.Schema.DataType,
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
