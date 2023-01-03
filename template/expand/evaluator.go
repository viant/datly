package expand

import (
	"fmt"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	"reflect"
)

const (
	Criteria    = "criteria"
	Logger      = "logger"
	HttpService = "http"
)

type (
	Evaluator struct {
		planner        *velty.Planner
		executor       *est.Execution
		stateProvider  func() *est.State
		constParams    []ConstUpdater
		paramSchema    reflect.Type
		presenceSchema reflect.Type
	}

	Type struct {
		Name  string
		RType reflect.Type
	}

	ConstUpdater interface {
		UpdateValue(params interface{}, presenceMap interface{}) error
	}
)

func NewEvaluator(consts []ConstUpdater, paramSchema, presenceSchema reflect.Type, template string, types ...*Type) (*Evaluator, error) {
	evaluator := &Evaluator{
		constParams:    consts,
		paramSchema:    paramSchema,
		presenceSchema: presenceSchema,
	}

	var err error
	evaluator.planner = velty.New(velty.BufferSize(len(template)), velty.PanicOnError(true))
	if err = evaluator.planner.DefineVariable(keywords.ParamsKey, paramSchema); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.ParamsMetadataKey, presenceSchema); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.KeyView, reflect.TypeOf(&MetaParam{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(
		Criteria, reflect.TypeOf(&SQLCriteria{}),
		keywords.KeySequencer, keywords.KeySQL,
	); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(Logger, reflect.TypeOf(&logger.Printer{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.KeyParentView, reflect.TypeOf(&MetaParam{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(HttpService, reflect.TypeOf(&Http{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(queryFunctionName, queryFnHandler); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(transformFunctionName, newTransform(types)); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(lengthFunctionName, newStringLength()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(lengthFunctionName, newArrayLength()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(queryFirstFunctionName, queryFirstFnHandler); err != nil {
		return nil, err
	}

	evaluator.executor, evaluator.stateProvider, err = evaluator.planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}

	return evaluator, nil
}

func (e *Evaluator) Evaluate(externalParams, presenceMap interface{}, viewParam *MetaParam, parentParam *MetaParam, logger *logger.Printer) (*est.State, *SQLCriteria, error) {
	if externalParams != nil {
		externalType := reflect.TypeOf(externalParams)
		if e.paramSchema != externalType {
			return nil, nil, fmt.Errorf("inompactible types, wanted %v got %T", e.paramSchema.String(), externalParams)
		}
	}

	externalParams, presenceMap = e.updateConsts(externalParams, presenceMap)

	newState := e.stateProvider()
	if externalParams != nil {
		if err := newState.SetValue(keywords.ParamsKey, externalParams); err != nil {
			return nil, nil, err
		}
	}

	if presenceMap != nil {
		if err := newState.SetValue(keywords.ParamsMetadataKey, presenceMap); err != nil {
			return nil, nil, err
		}
	}

	if err := newState.SetValue(keywords.KeyView, viewParam); err != nil {
		return nil, nil, err
	}

	if parentParam != nil {
		if err := newState.SetValue(keywords.KeyParentView, parentParam); err != nil {
			return nil, nil, err
		}
	}

	if err := newState.SetValue(Criteria, viewParam.sanitizer); err != nil {
		return nil, nil, err
	}

	if err := newState.SetValue(Logger, logger); err != nil {
		return nil, nil, err
	}

	if err := newState.SetValue(HttpService, &Http{}); err != nil {
		return nil, nil, err
	}

	if err := e.executor.Exec(newState); err != nil {
		return nil, nil, err
	}

	return newState, viewParam.sanitizer, nil
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
