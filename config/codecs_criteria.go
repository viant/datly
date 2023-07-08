package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xreflect"
	"reflect"
)

const CodecCriteriaBuilder = "CriteriaBuilder"

type (
	CriteriaBuilderFactory struct {
	}

	CriteriaBuilder struct {
		receiverType  reflect.Type
		columnsSource parameter.ColumnsSource
	}
)

func (c *CriteriaBuilderFactory) ResultType(paramType reflect.Type) (reflect.Type, error) {
	panic(UnexpectedUseError("ResultType", c))
}

func (c *CriteriaBuilderFactory) Valuer() Valuer {
	panic(UnexpectedUseError("Valuer", c))
}

func (c *CriteriaBuilder) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&parameter.Criteria{}), nil
}

func (c *CriteriaBuilderFactory) Name() string {
	return CodecCriteriaBuilder
}

func (c *CriteriaBuilderFactory) New(codecConfig *CodecConfig, _ reflect.Type, options ...interface{}) (Valuer, error) {
	if codecConfig.HandlerType == "" {
		panic("HandlerType can't be empty")
	}

	var typesLookup xreflect.LookupType
	var columnsSource parameter.ColumnsSource
	for _, option := range options {
		switch actual := option.(type) {
		case xreflect.LookupType:
			typesLookup = actual
		case parameter.ColumnsSource:
			columnsSource = actual
		}
	}

	lookupType, err := types.LookupType(typesLookup, codecConfig.HandlerType)
	if err != nil {
		panic(err)
	}

	_, ok := types.NewValue(lookupType).(parameter.CriteriaBuilder)
	if !ok {
		panic(fmt.Sprintf("expected %v to implement parameter.Criteria builder", codecConfig.HandlerType))
	}

	return &CriteriaBuilder{
		receiverType:  lookupType,
		columnsSource: columnsSource,
	}, nil
}

func (c *CriteriaBuilder) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	var valueGetter parameter.ValueGetter
	var selector parameter.Selector
	columnsSource := c.columnsSource
	for _, option := range options {
		switch actual := option.(type) {
		case parameter.ValueGetter:
			valueGetter = actual
		case parameter.Selector:
			selector = actual
		case parameter.ColumnsSource:
			columnsSource = columnsSource
		}
	}

	if selector == nil {
		panic(fmt.Errorf("expected selector to be not nil"))
	}

	value := types.NewValue(c.receiverType)
	builder := value.(parameter.CriteriaBuilder)

	criteria, err := builder.BuildCriteria(ctx, raw, &parameter.Options{
		Columns:    columnsSource,
		Parameters: valueGetter,
		Selector:   selector,
	})

	if err != nil {
		return nil, err
	}

	return criteria, nil
}
