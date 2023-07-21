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
		evaluator *expand.Evaluator
		ctxType   reflect.Type
		argsIndex map[int]*predicate.NamedArgument
		stateName string
	}

	predicateEvaluator struct {
		ctx       *expand.CustomContext
		evaluator *expand.Evaluator
		stateName string
	}
)

func (e *predicateEvaluator) Evaluate(ctx *expand.Context, paramValue interface{}) (*expand.State, error) {
	return e.evaluator.Evaluate(ctx, expand.WithParameters(paramValue, nil), expand.WithCustomContext(e.ctx))
}

func (c *predicateCache) get(predicateConfig *config.PredicateConfig, paramType reflect.Type, registry config.PredicateRegistry) (*predicateEvaluator, error) {
	aKey := predicateKey{name: predicateConfig.Name, paramType: paramType}
	provider, err := c.getEvaluatorProvider(predicateConfig, paramType, registry, aKey)
	if err != nil {
		return nil, err
	}

	return provider.new(predicateConfig)
}

func (c *predicateCache) getEvaluatorProvider(predicateConfig *config.PredicateConfig, paramType reflect.Type, registry config.PredicateRegistry, aKey predicateKey) (*predicateEvaluatorProvider, error) {
	value, ok := c.Map.Load(aKey)
	if ok {
		return value.(*predicateEvaluatorProvider), nil
	}

	p := &predicateEvaluatorProvider{stateName: "FilterValue"}
	err := p.init(predicateConfig, paramType, registry)
	if err != nil {
		return nil, err
	}

	c.Map.Store(aKey, p)
	return p, nil
}

func (p *predicateEvaluatorProvider) new(predicateConfig *config.PredicateConfig) (*predicateEvaluator, error) {
	dst := types.NewValue(p.ctxType)
	dstPtr := xunsafe.AsPointer(dst)
	for _, field := range predicateConfig.Args {
		argument, ok := p.argsIndex[field.Position]
		if !ok {
			return nil, fmt.Errorf("not found predicate arg %v", field.Position)
		}
		xunsafe.FieldByName(p.ctxType, argument.Name).SetString(dstPtr, field.Name)
	}

	customCtx := &expand.CustomContext{
		Type:  p.ctxType,
		Value: dst,
	}

	return &predicateEvaluator{
		ctx:       customCtx,
		evaluator: p.evaluator,
		stateName: p.stateName,
	}, nil
}

func (p *predicateEvaluatorProvider) init(predicateConfig *config.PredicateConfig, paramType reflect.Type, registry config.PredicateRegistry) error {
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
	stateName := "FilterValue"
	evaluator, err := expand.NewEvaluator(lookup.Source, expand.WithStateName(stateName), expand.WithParamSchema(paramType, nil), expand.WithCustomContexts(&expand.CustomContext{Type: ctxType}))
	if err != nil {
		return err
	}

	p.ctxType = ctxType
	p.evaluator = evaluator
	p.argsIndex = argsIndexed
	p.stateName = stateName
	return nil
}
