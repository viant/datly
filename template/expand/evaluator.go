package expand

import (
	"fmt"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/godiff"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	"github.com/viant/xreflect"
	"reflect"
)

type (
	Evaluator struct {
		planner          *velty.Planner
		executor         *est.Execution
		stateProvider    func() *est.State
		constParams      []ConstUpdater
		paramSchema      reflect.Type
		presenceSchema   reflect.Type
		supportsPresence bool
		supportsParams   bool
	}

	ConstUpdater interface {
		UpdateValue(params interface{}, presenceMap interface{}) error
	}
)

func NewEvaluator(consts []ConstUpdater, paramSchema, presenceSchema reflect.Type, template string, typeLookup xreflect.TypeLookupFn, options ...interface{}) (*Evaluator, error) {
	evaluator := &Evaluator{
		constParams:      consts,
		paramSchema:      paramSchema,
		presenceSchema:   presenceSchema,
		supportsPresence: presenceSchema != nil,
		supportsParams:   paramSchema != nil,
	}

	aCofnig := createConfig(options)

	var err error
	evaluator.planner = velty.New(velty.BufferSize(len(template)), aCofnig.panicOnError, velty.TypeParser(func(typeRepresentation string) (reflect.Type, error) {
		return typeLookup("", "", typeRepresentation)
	}))

	if evaluator.supportsParams {
		if err = evaluator.planner.DefineVariable(keywords.ParamsKey, paramSchema); err != nil {
			return nil, err
		}
	}

	if evaluator.supportsPresence {
		if err = evaluator.planner.DefineVariable(keywords.ParamsMetadataKey, presenceSchema); err != nil {
			return nil, err
		}
	}

	if err = evaluator.planner.EmbedVariable(Context{}); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnQuery, queryFnHandler); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnTransform, newTransform(typeLookup)); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnLength, newStringLength()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnLength, newArrayLength()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnQueryFirst, queryFirstFnHandler); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFuncNs(fnsDiffer, Differ{}); err != nil {
		return nil, err
	}

	for _, valueType := range aCofnig.valueTypes {
		if err = evaluator.planner.EmbedVariable(valueType.Type); err != nil {
			return nil, err
		}
	}

	if err = evaluator.planner.RegisterTypeFunc(reflect.TypeOf(&godiff.ChangeLog{}), funcChanged); err != nil {
		return nil, err
	}

	evaluator.executor, evaluator.stateProvider, err = evaluator.planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}

	return evaluator, nil
}

func createConfig(options []interface{}) *config {
	instance := newConfig()
	for _, option := range options {
		switch actual := option.(type) {
		case []*CustomContext:
			instance.valueTypes = append(instance.valueTypes, actual...)
		case *CustomContext:
			instance.valueTypes = append(instance.valueTypes, actual)
		case velty.PanicOnError:
			instance.panicOnError = actual
		}
	}

	return instance
}

func (e *Evaluator) Evaluate(externalParams, presenceMap interface{}, viewParam *MetaParam, parentParam *MetaParam, state *State, options ...interface{}) (*State, error) {
	if externalParams != nil {
		externalType := reflect.TypeOf(externalParams)
		if e.paramSchema != externalType {
			return nil, fmt.Errorf("inompactible types, wanted %v got %T", e.paramSchema.String(), externalParams)
		}
	}

	state = e.ensureState(state, viewParam, parentParam, goValidator)

	externalParams, presenceMap = e.updateConsts(externalParams, presenceMap)
	if externalParams != nil && e.supportsParams {
		if err := state.SetValue(keywords.ParamsKey, externalParams); err != nil {
			return nil, err
		}
	}

	if presenceMap != nil && e.supportsPresence {
		if err := state.SetValue(keywords.ParamsMetadataKey, presenceMap); err != nil {
			return nil, err
		}
	}

	if err := state.EmbedValue(state.Context); err != nil {
		return nil, err
	}

	for _, option := range options {
		switch actual := option.(type) {
		case *CustomContext:
			actualType := reflect.TypeOf(actual.Value)
			if actualType != actual.Type {
				return nil, fmt.Errorf("type missmatch, wanted %v got %v", actualType.String(), actual.Type.String())
			}

			if actual.Value != nil {
				if err := state.State.EmbedValue(actual.Value); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := e.executor.Exec(state.State); err != nil {
		return state, err
	}

	state.Expanded = state.Buffer.String()
	return state, nil
}

func (e *Evaluator) ensureState(state *State, param *MetaParam, parentParam *MetaParam, validator *Validator) *State {
	if state == nil {
		state = &State{}
	}

	state.Init(e.stateProvider(), param, parentParam, validator)
	return state
}

func (e *Evaluator) updateConsts(params interface{}, presenceMap interface{}) (interface{}, interface{}) {
	if len(e.constParams) == 0 {
		return params, presenceMap
	}

	if params == nil {
		params = reflect.New(e.paramSchema).Elem().Interface()
		presenceMap = reflect.New(e.presenceSchema).Elem().Interface()
	}

	for _, param := range e.constParams {
		_ = param.UpdateValue(params, presenceMap)
	}

	return params, presenceMap
}

func (e *Evaluator) Type() reflect.Type {
	return e.planner.Type.Type
}
