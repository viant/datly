package expand

import (
	"fmt"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/godiff"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	"github.com/viant/velty/est/op"
	"github.com/viant/xreflect"
	"reflect"
)

type (
	Evaluator struct {
		planner          *velty.Planner
		executor         *est.Execution
		stateProvider    func() *est.State
		constParams      []ConstUpdater
		predicateConfigs []*PredicateConfig
		paramSchema      reflect.Type
		presenceSchema   reflect.Type
		supportsPresence bool
		supportsParams   bool
		stateName        string
	}

	ConstUpdater interface {
		UpdateValue(params interface{}, presenceMap interface{}) error
	}

	EvaluatorOption func(c *config)
)

func WithCustomContexts(ctx ...*CustomContext) EvaluatorOption {
	return func(c *config) {
		c.valueTypes = append(c.valueTypes, ctx...)
	}
}

func WithPanicOnError(b bool) EvaluatorOption {
	return func(c *config) {
		c.panicOnError = true
	}
}

func WithConstUpdaters(updaters []ConstUpdater) EvaluatorOption {
	return func(c *config) {
		c.constUpdaters = updaters
	}
}

func WithTypeLookup(lookup xreflect.LookupType) EvaluatorOption {
	return func(c *config) {
		c.typeLookup = lookup
	}
}

func WithParamSchema(pSchema, hasSchema reflect.Type) EvaluatorOption {
	return func(c *config) {
		c.pSchema = pSchema
		c.hasSchema = hasSchema
	}
}

func WithStateName(name string) EvaluatorOption {
	return func(c *config) {
		c.stateName = name
	}
}

func NewEvaluator(template string, options ...EvaluatorOption) (*Evaluator, error) {
	aCofnig := createConfig(options)

	evaluator := &Evaluator{
		constParams:      aCofnig.constUpdaters,
		paramSchema:      aCofnig.pSchema,
		presenceSchema:   aCofnig.hasSchema,
		supportsPresence: aCofnig.hasSchema != nil,
		supportsParams:   aCofnig.pSchema != nil,
		stateName:        aCofnig.stateName,
		predicateConfigs: aCofnig.predicates,
	}

	var err error
	evaluator.planner = velty.New(velty.BufferSize(len(template)), aCofnig.panicOnError, velty.TypeParser(func(typeRepresentation string) (reflect.Type, error) {
		return aCofnig.typeLookup(typeRepresentation)
	}))

	if evaluator.supportsParams {
		if err = evaluator.planner.DefineVariable(aCofnig.stateName, aCofnig.pSchema); err != nil {
			return nil, err
		}
	}

	if evaluator.supportsPresence {
		if err = evaluator.planner.DefineVariable(keywords.ParamsMetadataKey, aCofnig.hasSchema); err != nil {
			return nil, err
		}
	}

	if err = evaluator.planner.EmbedVariable(Context{}); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnQuery, queryFnHandler); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnTransform, newTransform(aCofnig.typeLookup)); err != nil {
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

	aNewer := &newer{lookup: aCofnig.typeLookup}
	if err = evaluator.planner.RegisterStandaloneFunction(fnNew, &op.Function{
		Handler:     aNewer.New,
		ResultTyper: aNewer.NewResultType,
	}); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterStandaloneFunction(fnNop, &op.Function{
		Handler: noper{}.Nop,
	}); err != nil {
		return nil, err
	}

	evaluator.executor, evaluator.stateProvider, err = evaluator.planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}

	return evaluator, nil
}

func createConfig(options []EvaluatorOption) *config {
	instance := newConfig()
	for _, option := range options {
		option(instance)
	}

	return instance
}

func (e *Evaluator) Evaluate(ctx *Context, options ...StateOption) (*State, error) {
	state := e.ensureState(ctx, options...)
	externalParams, presenceMap := e.updateConsts(state.Parameters, state.ParametersHas)

	if externalParams != nil {
		externalType := reflect.TypeOf(externalParams)
		if e.paramSchema != externalType {
			return nil, fmt.Errorf("inompactible types, wanted %v got %T", e.paramSchema.String(), externalParams)
		}
	}

	if externalParams != nil && e.supportsParams {
		if err := state.SetValue(e.stateName, externalParams); err != nil {
			return nil, err
		}
	}

	if presenceMap != nil && e.supportsPresence {
		if err := state.SetValue(keywords.ParamsMetadataKey, presenceMap); err != nil {
			return nil, err
		}
	}

	if err := state.EmbedValue(*state.Context); err != nil {
		return nil, err
	}

	for _, customContext := range state.CustomContext {
		actualType := reflect.TypeOf(customContext.Value)
		if actualType != customContext.Type {
			return nil, fmt.Errorf("type missmatch, wanted %v got %v", actualType.String(), customContext.Type.String())
		}

		if customContext.Value != nil {
			if err := state.State.EmbedValue(customContext.Value); err != nil {
				return nil, err
			}
		}
	}

	if err := e.executor.Exec(state.State); err != nil {
		return state, err
	}

	state.Expanded = state.Buffer.String()
	return state, nil
}

func (e *Evaluator) ensureState(ctx *Context, options ...StateOption) *State {
	state := &State{
		Context: &Context{},
	}

	if ctx != nil {
		state.Context = ctx
	}

	state.Init(e.stateProvider(), e.predicateConfigs, options...)
	return state
}

func WithPredicates(configs []*PredicateConfig) EvaluatorOption {
	return func(cfg *config) {
		cfg.predicates = configs
	}
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
