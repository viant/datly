// Package template owns compiled SQL-template programs. It evaluates authored
// SQL control flow before sql/builder applies projection, criteria, relations,
// pagination, partitioning, and placeholder binding.
package template

import (
	"context"
	"fmt"
	"github.com/viant/datly/constant"
	"reflect"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/sql/fragment"
	sqlmacro "github.com/viant/datly/sql/macro"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	veltyparser "github.com/viant/velty/parser"
	xhandler "github.com/viant/xdatly/handler"
)

const (
	constantVariable = "SQLInstanceConstants"
	unsafeVariable   = "Unsafe"
	bindingVariable  = "SQLBindings"
	viewVariable     = "View"
	criteriaVariable = "criteria"
)

// Invocation contains the canonical bound input for one template evaluation.
type Invocation struct {
	Input  reflect.Value
	View   ViewInput
	Binder xhandler.Binder
}

// Result is the expanded authored SQL handed back to the SQL builder.
type Result struct {
	SQL  string
	Args []any
	// ParentBindings reports that $View rendered or excluded a parent-key
	// helper, so the builder must not run a second relation-binding path.
	ParentBindings bool
}

// Evaluator is the injected SQL-template execution boundary.
type Evaluator interface {
	Evaluate(context.Context, Invocation) (Result, error)
}

// Variable maps an authored DQL parameter name to one typed input field.
type Variable struct {
	Name       string
	FieldIndex []int
	Type       reflect.Type
	Key        xhandler.ValueKey
	Required   bool
}

// Compiler compiles one SQL source against its canonical input type.
type Compiler struct {
	// Const is immutable trusted deployment data, never invocation input.
	Const            *constant.Values
	Source           string
	InputType        reflect.Type
	Variables        []Variable
	NonWindowAliases []string
	Predicate        dexec.PredicateEvaluator
}

type compiled struct {
	constants       *constantBindings
	allowEmptyInput bool
	exec            *est.Execution
	newState        func() *est.State
	inputType       reflect.Type
	variables       []variableBinding
	predicate       dexec.PredicateEvaluator
}

type variableBinding struct {
	name            string
	placeholderName string
	fieldIndex      []int
	valueType       reflect.Type
	key             xhandler.ValueKey
	required        bool
}

// Compile returns nil for plain SQL and a reusable evaluator when the parsed
// source contains executable Velty control flow or SQL context calls.
func (c Compiler) Compile() (Evaluator, error) {
	source, constants, err := c.constantSource()
	if err != nil {
		return nil, err
	}
	protected := []string{dexec.PredicateVariable, viewVariable, unsafeVariable, criteriaVariable}
	for _, v := range c.Variables {
		protected = append(protected, v.Name)
	}
	protected = append(protected, c.Const.Names()...)
	source = protectContextVariables(source, protected...)
	source, _ = sqlmacro.PrepareNonWindowSQLTemplate(source, c.NonWindowAliases...)
	templateVariables := []string{dexec.PredicateVariable, viewVariable, unsafeVariable, criteriaVariable}
	for _, variable := range c.Variables {
		if name := strings.TrimSpace(variable.Name); name != "" {
			templateVariables = append(templateVariables, name)
		}
	}
	if constants == nil && !hasTemplateCode(source, templateVariables...) {
		return nil, nil
	}
	root, spans, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return nil, fmt.Errorf("parse SQL template: %w", err)
	}
	predicateUsed := usesVariable(root.Statements(), dexec.PredicateVariable)
	viewUsed := usesVariable(root.Statements(), viewVariable)
	unsafeUsed := usesVariable(root.Statements(), unsafeVariable)
	criteriaUsed := usesVariable(root.Statements(), criteriaVariable)
	declaredVariableUsed := false
	for _, variable := range c.Variables {
		if usesVariable(root.Statements(), strings.TrimSpace(variable.Name)) {
			declaredVariableUsed = true
			break
		}
	}
	if constants == nil && !hasControlFlow(root) && !predicateUsed && !viewUsed && !unsafeUsed && !criteriaUsed && !declaredVariableUsed {
		return nil, nil
	}
	if predicateUsed && c.Predicate == nil {
		return nil, fmt.Errorf("SQL template uses $%s without a compiled predicate program", dexec.PredicateVariable)
	}
	inputType := dereferenceType(c.InputType)
	allowEmptyInput := inputType == nil && constants != nil && !hasControlFlow(root) && !predicateUsed && !viewUsed && !unsafeUsed && !criteriaUsed && !declaredVariableUsed
	if allowEmptyInput {
		inputType = reflect.TypeFor[struct{}]()
	}
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("SQL template input must be a struct, got %v", c.InputType)
	}
	planner := velty.New(velty.BufferSize(len(source) + 128))
	if constants != nil {
		if err := planner.DefineVariable(constantVariable, &constantBindings{}); err != nil {
			return nil, fmt.Errorf("define SQL constant slots: %w", err)
		}
	}
	if err := planner.DefineVariable(unsafeVariable, reflect.New(inputType).Interface()); err != nil {
		return nil, fmt.Errorf("define $%s SQL template input: %w", unsafeVariable, err)
	}
	if err := planner.DefineVariable(bindingVariable, &fragment.Bindings{}); err != nil {
		return nil, fmt.Errorf("define $%s SQL template binding collector: %w", bindingVariable, err)
	}
	if err := planner.DefineVariable(criteriaVariable, fragment.New(nil)); err != nil {
		return nil, fmt.Errorf("define SQL criteria context: %w", err)
	}
	if err := planner.DefineVariable(viewVariable, &viewContext{}); err != nil {
		return nil, fmt.Errorf("define $%s SQL template context: %w", viewVariable, err)
	}
	if predicateUsed {
		contextType := c.Predicate.ContextType()
		if contextType == nil || contextType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("define $%s SQL template context: predicate context must be a struct, got %v", dexec.PredicateVariable, contextType)
		}
		if err := planner.DefineVariable(dexec.PredicateVariable, reflect.New(contextType).Interface()); err != nil {
			return nil, fmt.Errorf("define $%s SQL template context: %w", dexec.PredicateVariable, err)
		}
	}
	bindings, err := c.defineVariables(planner, inputType)
	if err != nil {
		return nil, err
	}
	source, err = bindEmittedInputVariables(source, root, spans, bindings)
	if err != nil {
		return nil, err
	}
	exec, newState, err := planner.Compile([]byte(source))
	if err != nil {
		return nil, fmt.Errorf("compile SQL template: %w", err)
	}
	var predicateProgram dexec.PredicateEvaluator
	if predicateUsed {
		predicateProgram = c.Predicate
	}
	return &compiled{constants: constants, allowEmptyInput: allowEmptyInput,
		exec:      exec,
		newState:  newState,
		inputType: inputType,
		variables: bindings,
		predicate: predicateProgram,
	}, nil
}

func (c Compiler) defineVariables(planner *velty.Planner, inputType reflect.Type) ([]variableBinding, error) {
	seen := map[string]bool{constantVariable: true, unsafeVariable: true, bindingVariable: true, viewVariable: true, criteriaVariable: true, dexec.PredicateVariable: true}
	variables := make([]Variable, 0, inputType.NumField()+len(c.Variables))
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		if !field.IsExported() {
			continue
		}
		variables = append(variables, Variable{Name: field.Name, FieldIndex: field.Index})
	}
	variables = append(variables, c.Variables...)
	result := make([]variableBinding, 0, len(variables))
	for _, variable := range variables {
		name := strings.TrimSpace(variable.Name)
		if name == "" || seen[name] {
			continue
		}
		valueType := variable.Type
		placeholderName := ""
		if len(variable.FieldIndex) > 0 {
			field, err := fieldByIndex(inputType, variable.FieldIndex)
			if err != nil {
				return nil, fmt.Errorf("define SQL template variable $%s: %w", name, err)
			}
			valueType = field.Type
			placeholderName = field.Name
		}
		if valueType == nil {
			return nil, fmt.Errorf("define SQL template variable $%s: type is required", name)
		}
		if len(variable.FieldIndex) == 0 && variable.Key == "" {
			return nil, fmt.Errorf("define SQL template variable $%s: binder key is required", name)
		}
		if err := planner.DefineVariable(name, variableDeclaration(valueType)); err != nil {
			return nil, fmt.Errorf("define SQL template variable $%s: %w", name, err)
		}
		seen[name] = true
		result = append(result, variableBinding{
			name:            name,
			placeholderName: placeholderName,
			fieldIndex:      append([]int(nil), variable.FieldIndex...),
			valueType:       valueType,
			key:             variable.Key,
			required:        variable.Required,
		})
	}
	return result, nil
}

func (c *compiled) Evaluate(ctx context.Context, invocation Invocation) (Result, error) {
	if c == nil || c.exec == nil || c.newState == nil {
		return Result{}, fmt.Errorf("compiled SQL template is not initialized")
	}
	input, inputPtr, err := c.input(invocation.Input)
	if err != nil {
		return Result{}, err
	}
	state := c.newState()
	state.SetContext(ctx)
	if c.constants != nil {
		if err := state.SetValue(constantVariable, c.constants); err != nil {
			return Result{}, fmt.Errorf("set SQL constant slots: %w", err)
		}
	}
	if err := state.SetValue(unsafeVariable, inputPtr.Interface()); err != nil {
		return Result{}, fmt.Errorf("set $%s SQL template input: %w", unsafeVariable, err)
	}
	bindings := &fragment.Bindings{}
	if err := state.SetValue(bindingVariable, bindings); err != nil {
		return Result{}, fmt.Errorf("set $%s SQL template binding collector: %w", bindingVariable, err)
	}
	if err := state.SetValue(criteriaVariable, fragment.New(bindings).WithDialect(invocation.View.Dialect)); err != nil {
		return Result{}, fmt.Errorf("set SQL criteria context: %w", err)
	}
	view := newViewContext(invocation.View, bindings)
	if err := state.SetValue(viewVariable, view); err != nil {
		return Result{}, fmt.Errorf("set $%s SQL template context: %w", viewVariable, err)
	}
	if c.predicate != nil {
		predicateContext, err := c.predicate.NewContext(fragment.WithDialect(ctx, invocation.View.Dialect), invocation.Binder, bindings.Append)
		if err != nil {
			return Result{}, fmt.Errorf("create $%s SQL template context: %w", dexec.PredicateVariable, err)
		}
		if err := state.SetValue(dexec.PredicateVariable, predicateContext); err != nil {
			return Result{}, fmt.Errorf("set $%s SQL template context: %w", dexec.PredicateVariable, err)
		}
	}
	for _, variable := range c.variables {
		value, err := variable.resolve(ctx, input, invocation.Binder)
		if err != nil {
			return Result{}, err
		}
		if err := state.SetValue(variable.name, variableValue(value)); err != nil {
			return Result{}, fmt.Errorf("set SQL template variable $%s: %w", variable.name, err)
		}
	}
	if err := c.exec.Exec(state); err != nil {
		return Result{}, fmt.Errorf("execute SQL template: %w", err)
	}
	return Result{
		SQL:            state.Buffer.String(),
		Args:           bindings.Args(),
		ParentBindings: view.parentExpanded,
	}, nil
}

func (v variableBinding) resolve(ctx context.Context, input reflect.Value, binder xhandler.Binder) (reflect.Value, error) {
	if len(v.fieldIndex) > 0 {
		value, err := input.FieldByIndexErr(v.fieldIndex)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("read SQL template variable $%s: %w", v.name, err)
		}
		return value, nil
	}
	if binder == nil {
		if v.required {
			return reflect.Value{}, fmt.Errorf("resolve SQL template variable $%s: binder is required", v.name)
		}
		return reflect.Zero(v.valueType), nil
	}
	actual, found, err := binder.Lookup(ctx, v.key)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("resolve SQL template variable $%s: %w", v.name, err)
	}
	if !found {
		if v.required {
			return reflect.Value{}, fmt.Errorf("resolve SQL template variable $%s: binder value %q is not available", v.name, v.key)
		}
		return reflect.Zero(v.valueType), nil
	}
	return typedVariableValue(v.name, v.valueType, actual)
}

func typedVariableValue(name string, target reflect.Type, actual any) (reflect.Value, error) {
	if actual == nil {
		switch target.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return reflect.Zero(target), nil
		default:
			return reflect.Value{}, fmt.Errorf("resolve SQL template variable $%s: nil is not assignable to %s", name, target)
		}
	}
	value := reflect.ValueOf(actual)
	if value.Type().AssignableTo(target) {
		return value, nil
	}
	if value.Type().ConvertibleTo(target) {
		return value.Convert(target), nil
	}
	return reflect.Value{}, fmt.Errorf("resolve SQL template variable $%s: binder value has type %s, not %s", name, value.Type(), target)
}

func (c *compiled) input(value reflect.Value) (reflect.Value, reflect.Value, error) {
	if !value.IsValid() && c.allowEmptyInput {
		value = reflect.ValueOf(struct{}{})
	}
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return reflect.Value{}, reflect.Value{}, fmt.Errorf("SQL template input is nil")
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return reflect.Value{}, reflect.Value{}, fmt.Errorf("SQL template input is required")
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}, reflect.Value{}, fmt.Errorf("SQL template input is nil")
		}
		if value.Type().Elem() != c.inputType {
			return reflect.Value{}, reflect.Value{}, fmt.Errorf("SQL template input must be *%s, got %s", c.inputType, value.Type())
		}
		return value.Elem(), value, nil
	}
	if value.Type() != c.inputType {
		return reflect.Value{}, reflect.Value{}, fmt.Errorf("SQL template input must be %s, got %s", c.inputType, value.Type())
	}
	inputPtr := reflect.New(c.inputType)
	inputPtr.Elem().Set(value)
	return inputPtr.Elem(), inputPtr, nil
}

func fieldByIndex(inputType reflect.Type, index []int) (reflect.StructField, error) {
	if len(index) == 0 {
		return reflect.StructField{}, fmt.Errorf("input field index is required")
	}
	current := inputType
	var field reflect.StructField
	for depth, fieldIndex := range index {
		current = dereferenceType(current)
		if current == nil || current.Kind() != reflect.Struct {
			return reflect.StructField{}, fmt.Errorf("input field index %v crosses non-struct type at depth %d", index, depth)
		}
		if fieldIndex < 0 || fieldIndex >= current.NumField() {
			return reflect.StructField{}, fmt.Errorf("input field index %v is out of range at depth %d", index, depth)
		}
		field = current.Field(fieldIndex)
		if !field.IsExported() {
			return reflect.StructField{}, fmt.Errorf("input field %s is not exported", field.Name)
		}
		current = field.Type
	}
	return field, nil
}

func dereferenceType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func variableDeclaration(fieldType reflect.Type) any {
	return fieldType
}

func variableValue(value reflect.Value) any {
	if value.Kind() == reflect.Struct {
		ptr := reflect.New(value.Type())
		ptr.Elem().Set(value)
		return ptr.Interface()
	}
	return value.Interface()
}
