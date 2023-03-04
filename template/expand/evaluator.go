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

var (
	Criteria       = keywords.ReservedKeywords.AddAndGet("criteria")
	Logger         = keywords.ReservedKeywords.AddAndGet("logger")
	Fmt            = keywords.ReservedKeywords.AddAndGet("fmt")
	FnsHttpService = keywords.ReservedKeywords.AddAndGet("http")
	ValidatorNs    = keywords.ReservedKeywords.AddAndGet("validator")
	Response       = keywords.ReservedKeywords.AddAndGet("response")
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
		UpdateValue(params interface{}, presenceMap interface{}) error
	}
)

func NewEvaluator(consts []ConstUpdater, paramSchema, presenceSchema reflect.Type, template string, typeLookup xreflect.TypeLookupFn) (*Evaluator, error) {
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
		Criteria, reflect.TypeOf(&DataUnit{}),
		keywords.KeySequencer,
		keywords.KeySQL,
		keywords.KeySQLx,
	); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(Logger, reflect.TypeOf(&Printer{}), Fmt); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(keywords.KeyParentView, reflect.TypeOf(&MetaParam{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(FnsHttpService, reflect.TypeOf(&Http{})); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(ValidatorNs, reflect.TypeOf(goValidator)); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(Response, reflect.TypeOf(&ResponseBuilder{})); err != nil {
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

	if err = evaluator.planner.RegisterFuncNs(FnsDiffer, Differ{}); err != nil {
		return nil, err
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

func (e *Evaluator) Evaluate(externalParams, presenceMap interface{}, viewParam *MetaParam, parentParam *MetaParam, state *State) (*State, error) {
	if externalParams != nil {
		externalType := reflect.TypeOf(externalParams)
		if e.paramSchema != externalType {
			return nil, fmt.Errorf("inompactible types, wanted %v got %T", e.paramSchema.String(), externalParams)
		}
	}

	state = e.ensureState(state, viewParam)
	externalParams, presenceMap = e.updateConsts(externalParams, presenceMap)
	if externalParams != nil {
		if err := state.SetValue(keywords.ParamsKey, externalParams); err != nil {
			return nil, err
		}
	}

	if presenceMap != nil {
		if err := state.SetValue(keywords.ParamsMetadataKey, presenceMap); err != nil {
			return nil, err
		}
	}

	if err := state.SetValue(keywords.KeyView, viewParam); err != nil {
		return nil, err
	}

	if parentParam != nil {
		if err := state.SetValue(keywords.KeyParentView, parentParam); err != nil {
			return nil, err
		}
	}

	if err := state.SetValue(Criteria, viewParam.dataUnit); err != nil {
		return nil, err
	}

	if err := state.SetValue(Logger, state.Printer); err != nil {
		return nil, err
	}

	if err := state.SetValue(FnsHttpService, state.Http); err != nil {
		return nil, err
	}

	if err := state.SetValue(ValidatorNs, goValidator); err != nil {
		return nil, err
	}

	if err := state.SetValue(Response, state.ResponseBuilder); err != nil {
		return nil, err
	}

	if err := e.executor.Exec(state.State); err != nil {
		return state, err
	}

	state.Expanded = state.Buffer.String()
	return state, nil
}

func (e *Evaluator) ensureState(state *State, param *MetaParam) *State {
	if state == nil {
		state = &State{}
	}

	state.Init(e.stateProvider(), param)
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
