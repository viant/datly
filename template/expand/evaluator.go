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
	Criteria = "criteria"
	Logger   = "logger"
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

	ConstUpdater interface {
		UpdateValue(params interface{}, presenceMap interface{})
	}
)

func NewEvaluator(consts []ConstUpdater, paramSchema, presenceSchema reflect.Type, template string) (*Evaluator, error) {
	evaluator := &Evaluator{
		constParams:    consts,
		paramSchema:    paramSchema,
		presenceSchema: presenceSchema,
	}

	var err error
	evaluator.planner = velty.New(velty.BufferSize(len(template)))
	if err = evaluator.planner.DefineVariable(keywords.ParamsKey, paramSchema); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.ParamsMetadataKey, presenceSchema); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.ViewKey, reflect.TypeOf(&MetaParam{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(Criteria, reflect.TypeOf(&CriteriaSanitizer{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(Logger, reflect.TypeOf(&logger.Printer{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.ParentViewKey, reflect.TypeOf(&MetaParam{})); err != nil {
		return nil, err
	}

	evaluator.executor, evaluator.stateProvider, err = evaluator.planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}

	return evaluator, nil
}

func (e *Evaluator) Evaluate(externalParams, presenceMap interface{}, viewParam *MetaParam, parentParam *MetaParam, logger *logger.Printer) (string, *CriteriaSanitizer, error) {
	if externalParams != nil {
		externalType := reflect.TypeOf(externalParams)
		if e.paramSchema != externalType {
			return "", nil, fmt.Errorf("inompactible types, wanted %v got %T", e.paramSchema.String(), externalParams)
		}
	}

	externalParams, presenceMap = e.updateConsts(externalParams, presenceMap)

	newState := e.stateProvider()
	if externalParams != nil {
		if err := newState.SetValue(keywords.ParamsKey, externalParams); err != nil {
			return "", nil, err
		}
	}

	if presenceMap != nil {
		if err := newState.SetValue(keywords.ParamsMetadataKey, presenceMap); err != nil {
			return "", nil, err
		}
	}

	if err := newState.SetValue(keywords.ViewKey, viewParam); err != nil {
		return "", nil, err
	}

	if parentParam != nil {
		if err := newState.SetValue(keywords.ParentViewKey, parentParam); err != nil {
			return "", nil, err
		}
	}

	if err := newState.SetValue(Criteria, viewParam.sanitizer); err != nil {
		return "", nil, err
	}

	if err := newState.SetValue(Logger, logger); err != nil {
		return "", nil, err
	}

	if err := e.executor.Exec(newState); err != nil {
		return "", nil, err
	}

	return newState.Buffer.String(), viewParam.sanitizer, nil
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
		param.UpdateValue(params, presenceMap)
	}

	return params, presenceMap
}

func ExpandWithDefaults(constParameters []ConstUpdater, paramSchema, presenceSchema reflect.Type, template string) (string, []interface{}, error) {
	evaluator, err := NewEvaluator(constParameters, paramSchema, presenceSchema, template)
	if err != nil {
		return "", nil, err
	}

	evaluate, sanitizer, err := evaluator.Evaluate(NewValue(paramSchema), NewValue(presenceSchema), MockMetaParam(), MockMetaParam(), &logger.Printer{})
	if err != nil {
		return "", nil, err
	}

	return evaluate, sanitizer.ParamsGroup, nil
}
