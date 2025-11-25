package view

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
)

type (
	predicateCache struct {
		Map sync.Map
	}

	predicateKey struct {
		name      string
		paramType reflect.Type
	}

	predicateEvaluatorProvider struct {
		evaluator    *expand.Evaluator
		ctxType      reflect.Type
		signature    map[int]*predicate.NamedArgument
		state        *expand.NamedVariable
		hasStateName *expand.NamedVariable
		handler      codec.PredicateHandler
		stateType    *structology.StateType
	}

	PredicateEvaluator struct {
		ctx           *expand.Variable
		evaluator     *expand.Evaluator
		valueState    *expand.NamedVariable
		hasValueState *expand.NamedVariable
		stateType     *structology.StateType
	}
)

func (e *PredicateEvaluator) Compute(ctx context.Context, value interface{}) (*codec.Criteria, error) {
	cuxtomCtx, ok := ctx.Value(expand.PredicateCtx).(*expand.Context)
	if !ok {
		panic("not found custom ctx")
	}

	val := ctx.Value(expand.PredicateState)
	var aState *structology.State
	if s, ok := val.(*structology.State); ok {
		aState = s
	}
	if aState == nil && e.stateType != nil {
		// Initialize state if absent; do not override if provided.
		aState = e.stateType.NewState()
	}
	//  evaluate predicate with an isolated DataUnit to avoid
	// mutating parent DataUnit and relying on Shrink/restore across nesting.
	var metaSource expand.Dber
	if cuxtomCtx.DataUnit != nil {
		metaSource = cuxtomCtx.DataUnit.MetaSource
	}
	isolatedDU := expand.NewDataUnit(metaSource)
	tmpCtx := *cuxtomCtx
	tmpCtx.DataUnit = isolatedDU

	evaluate, err := e.Evaluate(&tmpCtx, aState, value)
	if err != nil {
		return nil, err
	}

	// Collect placeholders from the isolated DataUnit and return them
	// to the caller; do not mutate the parent DataUnit here.
	values := make([]interface{}, len(isolatedDU.ParamsGroup))
	copy(values, isolatedDU.ParamsGroup)
	criteria := &codec.Criteria{Expression: evaluate.Buffer.String(), Placeholders: values}
	return criteria, nil
}

func (e *PredicateEvaluator) Evaluate(ctx *expand.Context, state *structology.State, value interface{}) (*expand.State, error) {

	hasValue := value != nil
	if value != nil {
		switch actual := value.(type) {
		case []int:
			hasValue = actual != nil
		case *int:
			hasValue = actual != nil
		case *string:
			hasValue = actual != nil
		}
	}
	return e.evaluator.Evaluate(ctx,
		expand.WithParameterState(state),
		expand.WithNamedVariables(
			e.valueState.New(value),
			e.hasValueState.New(hasValue),
		),
		expand.WithCustomContext(e.ctx),
	)
}

func (c *predicateCache) get(resource *Resource, predicateConfig *extension.PredicateConfig, param *state.Parameter, registry *extension.PredicateRegistry, stateType *structology.StateType) (codec.PredicateHandler, error) {
	keyName := predicateConfig.Name
	if isCustomPredicate(keyName) {
		keyName += strings.Join(predicateConfig.Args, ",")
	}
	outputType := param.OutputType()
	aKey := predicateKey{name: keyName, paramType: outputType}
	var provider, err = c.getEvaluatorProvider(resource, predicateConfig, outputType, registry, aKey, stateType)
	if err != nil {
		return nil, err
	}
	return provider.new(predicateConfig)
}

func isCustomPredicate(keyName string) bool {
	return keyName == "handler"
}

func (c *predicateCache) getEvaluatorProvider(resource *Resource, predicateConfig *extension.PredicateConfig, param reflect.Type, registry *extension.PredicateRegistry, aKey predicateKey, stateType *structology.StateType) (*predicateEvaluatorProvider, error) {
	value, ok := c.Map.Load(aKey)
	if ok {
		return value.(*predicateEvaluatorProvider), nil
	}

	p := &predicateEvaluatorProvider{}
	err := p.init(resource, predicateConfig, param, registry, stateType)
	if err != nil {
		return nil, err
	}

	c.Map.Store(aKey, p)
	return p, nil
}

func (p *predicateEvaluatorProvider) new(predicateConfig *extension.PredicateConfig) (codec.PredicateHandler, error) {
	if p.handler != nil {
		return p.handler, nil
	}

	dst := types.NewValue(p.ctxType)
	dstPtr := xunsafe.AsPointer(dst)
	for i, arg := range predicateConfig.Args {
		argument, ok := p.signature[i]
		if !ok {
			return nil, fmt.Errorf("not found predicate arg %v", i)
		}
		xunsafe.FieldByName(p.ctxType, argument.Name).SetString(dstPtr, arg)
	}

	customCtx := &expand.Variable{
		Type:  p.ctxType,
		Value: dst,
	}

	return &PredicateEvaluator{
		ctx:           customCtx,
		evaluator:     p.evaluator,
		valueState:    p.state,
		hasValueState: p.hasStateName,
		stateType:     p.stateType,
	}, nil
}

func (p *predicateEvaluatorProvider) init(resource *Resource, predicateConfig *extension.PredicateConfig, paramType reflect.Type, registry *extension.PredicateRegistry, stateType *structology.StateType) error {
	lookup, err := registry.Lookup(predicateConfig.Name)
	if err != nil {
		return err
	}

	if lookup.Handler != nil {
		handler, err := lookup.Handler.New(resource.LookupType(), predicateConfig.Args...)
		if err != nil {
			return err
		}

		p.handler = handler
		return nil
	}

	var ctxFields []reflect.StructField
	argsIndexed := map[int]*predicate.NamedArgument{}
	for _, arg := range lookup.Template.Args {
		ctxFields = append(ctxFields, state.NewField("", arg.Name, xreflect.StringType))
		argsIndexed[arg.Position] = arg
	}

	ctxType := reflect.StructOf(ctxFields)
	stateVariable := &expand.NamedVariable{
		Variable: expand.Variable{
			Type: paramType,
		},
		Name: "FilterValue",
	}
	hasVariable := &expand.NamedVariable{
		Variable: expand.Variable{
			Type: xreflect.BoolType,
		},
		Name: "HasFilterValue",
	}

	evaluator, err := expand.NewEvaluator(lookup.Template.Source,
		expand.WithStateType(stateType),
		expand.WithCustomContexts(&expand.Variable{Type: ctxType}),
		expand.WithVariable(
			stateVariable,
			hasVariable,
		),
	)
	if err != nil {
		return err
	}

	p.ctxType = ctxType
	p.evaluator = evaluator
	p.signature = argsIndexed
	p.state = stateVariable
	p.hasStateName = hasVariable
	p.stateType = stateType
	return nil
}
