package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"sync"
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
	}

	predicateEvaluator struct {
		ctx           *expand.Variable
		evaluator     *expand.Evaluator
		valueState    *expand.NamedVariable
		hasValueState *expand.NamedVariable
	}
)

func (e *predicateEvaluator) Compute(ctx context.Context, value interface{}) (*codec.Criteria, error) {
	cuxtomCtx, ok := ctx.Value(expand.PredicateCtx).(*expand.Context)
	if !ok {
		panic("not found custom ctx")
	}

	val := ctx.Value(expand.PredicateState)
	state := val.(*structology.State)
	evaluate, err := e.Evaluate(cuxtomCtx, state, value)
	if err != nil {
		return nil, err
	}

	return &codec.Criteria{Predicate: evaluate.Buffer.String()}, nil
}

func (e *predicateEvaluator) Evaluate(ctx *expand.Context, state *structology.State, value interface{}) (*expand.State, error) {
	return e.evaluator.Evaluate(ctx,
		expand.WithParameterState(state),
		expand.WithNamedVariables(
			e.valueState.New(value),
			e.hasValueState.New(value != nil),
		),
		expand.WithCustomContext(e.ctx),
	)
}

func (c *predicateCache) get(resource *Resource, predicateConfig *config.PredicateConfig, param *state.Parameter, registry *config.PredicateRegistry, stateType *structology.StateType) (codec.PredicateHandler, error) {
	keyName := predicateConfig.Name
	if isCustomPredicate(keyName) {
		keyName += strings.Join(predicateConfig.Args, ",")
	}
	aKey := predicateKey{name: keyName, paramType: param.OutputType()}
	var provider, err = c.getEvaluatorProvider(resource, predicateConfig, param.OutputType(), registry, aKey, stateType)
	if err != nil {
		return nil, err
	}

	return provider.new(predicateConfig)
}

func isCustomPredicate(keyName string) bool {
	return keyName == "handler"
}

func (c *predicateCache) getEvaluatorProvider(resource *Resource, predicateConfig *config.PredicateConfig, param reflect.Type, registry *config.PredicateRegistry, aKey predicateKey, stateType *structology.StateType) (*predicateEvaluatorProvider, error) {
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

func (p *predicateEvaluatorProvider) new(predicateConfig *config.PredicateConfig) (codec.PredicateHandler, error) {
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

	return &predicateEvaluator{
		ctx:           customCtx,
		evaluator:     p.evaluator,
		valueState:    p.state,
		hasValueState: p.hasStateName,
	}, nil
}

func (p *predicateEvaluatorProvider) init(resource *Resource, predicateConfig *config.PredicateConfig, paramType reflect.Type, registry *config.PredicateRegistry, stateType *structology.StateType) error {
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
	return nil
}
