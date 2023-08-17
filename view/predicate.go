package view

import (
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

type (
	predicateCache struct {
		sync.Map
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
	}

	predicateEvaluator struct {
		ctx           *expand.Variable
		evaluator     *expand.Evaluator
		valueState    *expand.NamedVariable
		hasValueState *expand.NamedVariable
	}
)

func (e *predicateEvaluator) Evaluate(ctx *expand.Context, state, hasState, paramValue interface{}) (*expand.State, error) {
	return e.evaluator.Evaluate(ctx,
		expand.WithParameters(state, hasState),
		expand.WithNamedVariables(
			e.valueState.New(paramValue),
			e.hasValueState.New(paramValue != nil),
		),
		expand.WithCustomContext(e.ctx),
	)
}

func (c *predicateCache) get(predicateConfig *config.PredicateConfig, param *Parameter, registry *config.PredicateRegistry, stateType, presenceType reflect.Type) (*predicateEvaluator, error) {
	aKey := predicateKey{name: predicateConfig.Name, paramType: param.ActualParamType()}
	var provider, err = c.getEvaluatorProvider(predicateConfig, param.ActualParamType(), registry, aKey, stateType, presenceType)
	if err != nil {
		return nil, err
	}

	return provider.new(predicateConfig)
}

func (c *predicateCache) getEvaluatorProvider(predicateConfig *config.PredicateConfig, param reflect.Type, registry *config.PredicateRegistry, aKey predicateKey, stateType reflect.Type, presenceType reflect.Type) (*predicateEvaluatorProvider, error) {
	value, ok := c.Map.Load(aKey)
	if ok {
		return value.(*predicateEvaluatorProvider), nil
	}

	p := &predicateEvaluatorProvider{}
	err := p.init(predicateConfig, param, registry, stateType, presenceType)
	if err != nil {
		return nil, err
	}

	c.Map.Store(aKey, p)
	return p, nil
}

func (p *predicateEvaluatorProvider) new(predicateConfig *config.PredicateConfig) (*predicateEvaluator, error) {
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

func (p *predicateEvaluatorProvider) init(predicateConfig *config.PredicateConfig, paramType reflect.Type, registry *config.PredicateRegistry, stateType reflect.Type, presenceType reflect.Type) error {
	lookup, err := registry.Lookup(predicateConfig.Name)
	if err != nil {
		return err
	}

	var ctxFields []reflect.StructField
	argsIndexed := map[int]*predicate.NamedArgument{}
	for _, arg := range lookup.Args {
		ctxFields = append(ctxFields, newField("", arg.Name, xreflect.StringType))
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

	evaluator, err := expand.NewEvaluator(lookup.Source,
		expand.WithParamSchema(stateType, presenceType),
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
