package velty

import (
	"fmt"
	"reflect"
	"sync"

	vtemplate "github.com/viant/velty"
	"github.com/viant/velty/est"
)

type compiledProgram[S any, I any, O any] struct {
	exec         *est.Execution
	newState     func() *est.State
	capabilities programCapabilities
}

type compiledProgramCacheKey struct {
	template   string
	stateType  reflect.Type
	inputType  reflect.Type
	outputType reflect.Type
}

var compiledProgramCache sync.Map

func newCompiledProgram[S any, I any, O any](template string) (result *compiledProgram[S, I, O], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("compile velty program: %v", recovered)
		}
	}()
	stateType, err := compiledProgramStateType[S]()
	if err != nil {
		return nil, err
	}
	inputType, err := compiledProgramStructType[I]("input")
	if err != nil {
		return nil, err
	}
	outputType, err := compiledProgramStructType[O]("output")
	if err != nil {
		return nil, err
	}
	capabilities, err := inspectProgramCapabilities(template)
	if err != nil {
		return nil, err
	}

	// Handler capability errors must stop subsequent effects, like a Go
	// handler returning its first error. Native Exec recovers this control flow.
	planner := vtemplate.New(vtemplate.BufferSize(len(template)+128), vtemplate.PanicOnError(true))
	if err := planner.EmbedVariable(reflect.New(stateType).Elem().Interface()); err != nil {
		return nil, err
	}
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		if !field.IsExported() {
			continue
		}
		if err := planner.DefineVariable(field.Name, compiledProgramVarType(field.Type)); err != nil {
			return nil, err
		}
	}
	if err := planner.DefineVariable("Input", reflect.New(inputType).Interface(), "input"); err != nil {
		return nil, err
	}
	if err := planner.DefineVariable("Output", reflect.New(outputType).Interface()); err != nil {
		return nil, err
	}
	exec, newState, err := planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}
	return &compiledProgram[S, I, O]{exec: exec, newState: newState, capabilities: capabilities}, nil
}

func cachedCompiledProgram[S any, I any, O any](template string) (*compiledProgram[S, I, O], error) {
	stateType, err := compiledProgramStateType[S]()
	if err != nil {
		return nil, err
	}
	inputType, err := compiledProgramStructType[I]("input")
	if err != nil {
		return nil, err
	}
	outputType, err := compiledProgramStructType[O]("output")
	if err != nil {
		return nil, err
	}
	key := compiledProgramCacheKey{template: template, stateType: stateType, inputType: inputType, outputType: outputType}
	if cached, ok := compiledProgramCache.Load(key); ok {
		if actual, cast := cached.(*compiledProgram[S, I, O]); cast {
			return actual, nil
		}
	}
	program, err := newCompiledProgram[S, I, O](template)
	if err != nil {
		return nil, err
	}
	actual, _ := compiledProgramCache.LoadOrStore(key, program)
	cached, ok := actual.(*compiledProgram[S, I, O])
	if !ok {
		return nil, fmt.Errorf("unexpected cached velty program type %T", actual)
	}
	return cached, nil
}

func (p *compiledProgram[S, I, O]) Exec(state *S, input *I, output *O) error {
	if p == nil || p.exec == nil || p.newState == nil {
		return fmt.Errorf("compiled velty program is not initialized")
	}
	if state == nil {
		return fmt.Errorf("compiled velty program state is required")
	}
	currentState := p.newState()
	if err := currentState.EmbedValue(*state); err != nil {
		return err
	}
	inputValue := reflect.ValueOf(input)
	if inputValue.Kind() == reflect.Ptr {
		if inputValue.IsNil() {
			return fmt.Errorf("compiled velty program input is nil")
		}
		inputValue = inputValue.Elem()
	}
	for i := 0; i < inputValue.NumField(); i++ {
		field := inputValue.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		if err := currentState.SetValue(field.Name, compiledProgramVarValue(inputValue.Field(i))); err != nil {
			return err
		}
	}
	if err := currentState.SetValue("Output", output); err != nil {
		return err
	}
	if err := currentState.SetValue("Input", input); err != nil {
		return err
	}
	return p.exec.Exec(currentState)
}

func compiledProgramStateType[S any]() (reflect.Type, error) {
	stateType := reflect.TypeFor[S]()
	if stateType == nil || stateType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("velty program state must be a struct, got %v", stateType)
	}
	return stateType, nil
}

func compiledProgramStructType[T any](name string) (reflect.Type, error) {
	rType := reflect.TypeFor[T]()
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType == nil || rType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("velty program %s must be a struct, got %v", name, reflect.TypeFor[T]())
	}
	return rType, nil
}

func compiledProgramVarType(fieldType reflect.Type) any {
	if fieldType == nil {
		return nil
	}
	if fieldType.Kind() == reflect.Struct {
		return reflect.New(fieldType).Interface()
	}
	return reflect.Zero(fieldType).Interface()
}

func compiledProgramVarValue(field reflect.Value) any {
	if !field.IsValid() {
		return nil
	}
	if field.Kind() == reflect.Struct {
		ptr := reflect.New(field.Type())
		ptr.Elem().Set(field)
		return ptr.Interface()
	}
	return field.Interface()
}
