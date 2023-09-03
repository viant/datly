package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	expand2 "github.com/viant/datly/service/executor/expand"
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
		evaluator    *expand2.Evaluator
		ctxType      reflect.Type
		signature    map[int]*predicate.NamedArgument
		state        *expand2.NamedVariable
		hasStateName *expand2.NamedVariable
		handler      codec.PredicateHandler
	}

	predicateEvaluator struct {
		ctx           *expand2.Variable
		evaluator     *expand2.Evaluator
		valueState    *expand2.NamedVariable
		hasValueState *expand2.NamedVariable
	}
)

func (e *predicateEvaluator) Compute(ctx context.Context, value interface{}) (*codec.Criteria, error) {
	cuxtomCtx, ok := ctx.Value(expand2.PredicateCtx).(*expand2.Context)
	if !ok {
		panic("not found custom ctx")
	}

	val := ctx.Value(expand2.PredicateState)
	aState := val.(*structology.State)
	offset := len(cuxtomCtx.DataUnit.ParamsGroup)
	evaluate, err := e.Evaluate(cuxtomCtx, aState, value)
	if err != nil {
		return nil, err
	}

	return &codec.Criteria{Expression: evaluate.Buffer.String(), Placeholders: evaluate.DataUnit.ParamsGroup[offset:]}, nil
}

func (e *predicateEvaluator) Evaluate(ctx *expand2.Context, state *structology.State, value interface{}) (*expand2.State, error) {
	return e.evaluator.Evaluate(ctx,
		expand2.WithParameterState(state),
		expand2.WithNamedVariables(
			e.valueState.New(value),
			e.hasValueState.New(value != nil),
		),
		expand2.WithCustomContext(e.ctx),
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

	customCtx := &expand2.Variable{
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
	stateVariable := &expand2.NamedVariable{
		Variable: expand2.Variable{
			Type: paramType,
		},
		Name: "FilterValue",
	}
	hasVariable := &expand2.NamedVariable{
		Variable: expand2.Variable{
			Type: xreflect.BoolType,
		},
		Name: "HasFilterValue",
	}

	evaluator, err := expand2.NewEvaluator(lookup.Template.Source,
		expand2.WithStateType(stateType),
		expand2.WithCustomContexts(&expand2.Variable{Type: ctxType}),
		expand2.WithVariable(
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
