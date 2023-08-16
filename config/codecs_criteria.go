package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/parameter"
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

func (c *CriteriaBuilderFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if err := ValidateArgs(codecConfig, 1, CodecCriteriaBuilder); err != nil {
		return nil, err
	}

	opts := NewOptions(codec.NewOptions(options))
	columnsSource := opts.ColumnsSource
	handlerType := codecConfig.Args[0]
	lookupType, err := types.LookupType(opts.LookupType, handlerType)
	if err != nil {
		panic(err)
	}
	_, ok := types.NewValue(lookupType).(parameter.CriteriaBuilder)
	if !ok {
		panic(fmt.Sprintf("expected %v to implement parameter.Criteria builder", handlerType))
	}

	return &CriteriaBuilder{
		receiverType:  lookupType,
		columnsSource: columnsSource,
	}, nil
}

func (c *CriteriaBuilder) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&parameter.Criteria{}), nil
}

func ValidateArgs(codecConfig *codec.Config, expectedLen int, codecName string) error {
	if len(codecConfig.Args) != expectedLen {
		return fmt.Errorf("expected %v to receive %v argument(s) but got %v", codecName, expectedLen, len(codecConfig.Args))
	}

	return nil
}

func ValidateMinArgs(config *codec.Config, name string, minLen int) error {
	if len(config.Args) < minLen {
		return fmt.Errorf("expected %v to receive %v argument(s) but got %v", name, minLen, len(config.Args))
	}

	return nil
}

func (c *CriteriaBuilder) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	var valueGetter parameter.ValueGetter
	var selector parameter.Selector
	columnsSource := c.columnsSource

	opts := codec.NewOptions(options)

	for _, option := range opts.Options {
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
