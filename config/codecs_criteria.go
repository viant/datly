package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const CodecCriteriaBuilder = "CriteriaBuilder"

type (
	CriteriaBuilderFactory struct {
	}

	CriteriaBuilder struct {
		receiverType  reflect.Type
		columnsSource codec.ColumnsSource
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
	_, ok := types.NewValue(lookupType).(codec.CriteriaBuilder)
	if !ok {
		panic(fmt.Sprintf("expected %v to implement parameter.Criteria builder", handlerType))
	}

	return &CriteriaBuilder{
		receiverType:  lookupType,
		columnsSource: columnsSource,
	}, nil
}

func (c *CriteriaBuilder) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&codec.Criteria{}), nil
}

func ValidateArgs(codecConfig *codec.Config, expectedLen int, codecName string) error {
	if len(codecConfig.Args) != expectedLen {
		return UnexpectedArgsLenError(codecConfig.Args, expectedLen, codecName)
	}

	return nil
}

func UnexpectedArgsLenError(got []string, expectedLen int, codecName string) error {
	return fmt.Errorf("expected %v to receive %v argument(s) but got %v", codecName, expectedLen, len(got))
}

func ValidateMinArgs(config *codec.Config, name string, minLen int) error {
	if len(config.Args) < minLen {
		return NotEnoughParametersError(config.Args, name, minLen)
	}

	return nil
}

func NotEnoughParametersError(got []string, name string, minLen int) error {
	return fmt.Errorf("expected %v to receive at least %v argument(s) but got %v", name, minLen, len(got))
}

func (c *CriteriaBuilder) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	var valueGetter codec.ValueGetter
	var selector codec.Selector
	columnsSource := c.columnsSource

	opts := codec.NewOptions(options)

	for _, option := range opts.Options {
		switch actual := option.(type) {
		case codec.ValueGetter:
			valueGetter = actual
		case codec.Selector:
			selector = actual
		case codec.ColumnsSource:
			columnsSource = columnsSource
		}
	}
	if opts.ColumnsSource != nil {
		columnsSource = opts.ColumnsSource
	}
	if opts.Selector != nil {
		selector = opts.Selector
	}
	if opts.ValueGetter != nil {
		valueGetter = opts.ValueGetter
	}

	if selector == nil {
		panic(fmt.Errorf("expected selector to be not nil"))
	}

	value := types.NewValue(c.receiverType)
	builder := value.(codec.CriteriaBuilder)

	criteria, err := builder.BuildCriteria(ctx, raw, &codec.CriteriaBuilderOptions{
		Columns:    columnsSource,
		Parameters: valueGetter,
		Selector:   selector,
	})

	if err != nil {
		return nil, err
	}

	return criteria, nil
}
