package expand

import (
	"fmt"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/godiff"
	"github.com/viant/structology"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	"github.com/viant/velty/est/op"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"reflect"
)

type (
	Evaluator struct {
		planner          *velty.Planner
		executor         *est.Execution
		stateProvider    func() *est.State
		setLiterals      func(state *structology.State) error
		stateType        *structology.StateType
		predicateConfigs []*PredicateConfig
		stateName        string
	}

	ConstUpdater interface {
		UpdateValue(params interface{}, presenceMap interface{}) error
	}

	EvaluatorOption func(c *config)
)

func WithCustomContexts(ctx ...*Variable) EvaluatorOption {
	return func(c *config) {
		c.embededTypes = append(c.embededTypes, ctx...)
	}
}

func WithVariable(namedVariable ...*NamedVariable) EvaluatorOption {
	return func(c *config) {
		c.namedVariables = append(c.namedVariables, namedVariable...)
	}
}

func WithPanicOnError(b bool) EvaluatorOption {
	return func(c *config) {
		c.panicOnError = true
	}
}

func WithSetLiteral(setLiterals func(state *structology.State) error) EvaluatorOption {
	return func(c *config) {
		c.setLiterals = setLiterals
	}
}

func WithTypeLookup(lookup xreflect.LookupType) EvaluatorOption {
	return func(c *config) {
		c.typeLookup = lookup
	}
}

func WithStateType(stateType *structology.StateType) EvaluatorOption {
	return func(c *config) {
		c.stateType = stateType
	}
}

func WithStateName(name string) EvaluatorOption {
	return func(c *config) {
		c.stateName = name
	}
}

func NewEvaluator(template string, options ...EvaluatorOption) (*Evaluator, error) {
	aConfig := createConfig(options)

	evaluator := &Evaluator{
		setLiterals:      aConfig.setLiterals,
		stateType:        aConfig.stateType,
		stateName:        aConfig.stateName,
		predicateConfigs: aConfig.predicates,
	}

	var err error
	evaluator.planner = velty.New(velty.BufferSize(len(template)), aConfig.panicOnError, velty.TypeParser(func(typeRepresentation string) (reflect.Type, error) {
		return aConfig.typeLookup(typeRepresentation)
	}))

	if evaluator.stateType != nil {
		if err = evaluator.planner.DefineVariable(aConfig.stateName, evaluator.stateType.Type()); err != nil {
			return nil, err
		}
		if evaluator.stateType.HasMarker() {
			marker := evaluator.stateType.Marker()
			if err = evaluator.planner.DefineVariable(keywords.SetMarkerKey, marker.Type()); err != nil {
				return nil, err
			}
		}
	}

	if err = evaluator.planner.EmbedVariable(Context{Filters: predicate.Filters{}}); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnQuery, queryFnHandler); err != nil {
		return nil, err
	}

	if err = evaluator.planner.RegisterFunctionKind(fnTransform, newTransform(aConfig.typeLookup)); err != nil {
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

	for _, valueType := range aConfig.embededTypes {
		if err = evaluator.planner.EmbedVariable(valueType.Type); err != nil {
			return nil, err
		}
	}

	for _, variable := range aConfig.namedVariables {
		if err = evaluator.planner.DefineVariable(variable.Name, variable.Type); err != nil {
			return nil, err
		}
	}

	if err = evaluator.planner.RegisterTypeFunc(reflect.TypeOf(&godiff.ChangeLog{}), funcChanged); err != nil {
		return nil, err
	}

	aNewer := &newer{lookup: aConfig.typeLookup}
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

	if state.ParametersState != nil && state.ParametersState.Type().IsDefined() {
		if e.setLiterals != nil {
			if err := e.setLiterals(state.ParametersState); err != nil {
				return nil, err
			}
		}
		externalType := state.ParametersState.Type().Type()
		if e.stateType.Type() != externalType {
			return nil, fmt.Errorf("inompactible types, wanted %v got %T", e.stateType.Type().String(), externalType.String())
		}
		if err := state.SetValue(e.stateName, state.ParametersState.State()); err != nil {
			return nil, err
		}
		if state.ParametersState.HasMarker() {
			if err := state.SetValue(keywords.SetMarkerKey, state.ParametersState.MarkerHolder()); err != nil {
				return nil, err
			}
		}

	}

	if err := state.EmbedValue(*state.Context); err != nil {
		return nil, err
	}

	for _, embededVariable := range state.EmbededVariables {
		if embededVariable.Value != nil {
			if err := state.State.EmbedValue(embededVariable.Value); err != nil {
				return nil, err
			}
		}
	}

	for _, namedVariable := range state.NamedVariables {
		if namedVariable.Value != nil {
			if err := state.State.SetValue(namedVariable.Name, namedVariable.Value); err != nil {
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
		Context: &Context{Filters: predicate.Filters{}},
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

func (e *Evaluator) Type() reflect.Type {
	return e.planner.Type.Type
}
