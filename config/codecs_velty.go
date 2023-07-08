package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/template/expand"
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const CodecVeltyCriteria = "VeltyCriteria"

type (
	VeltyCriteriaFactory struct {
	}

	VeltyCodec struct {
		template  string
		codecType reflect.Type
		evaluator *expand.Evaluator
		columns   parameter.ColumnsSource
	}
)

func (v *VeltyCodec) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&parameter.Criteria{}), nil
}

func (v *VeltyCriteriaFactory) Name() string {
	return CodecVeltyCriteria
}

func (v *VeltyCriteriaFactory) New(codecConfig *CodecConfig, rType reflect.Type, options ...interface{}) (Valuer, error) {
	var columnsIndex parameter.ColumnsSource
	for _, option := range options {
		switch actual := option.(type) {
		case parameter.ColumnsSource:
			columnsIndex = actual
		}
	}

	codec := &VeltyCodec{
		template:  codecConfig.Source,
		codecType: rType,
		columns:   columnsIndex,
	}

	if err := codec.init(); err != nil {
		return nil, err
	}

	return codec, nil
}

func (v *VeltyCodec) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to get string but got %T", raw)
	}

	rawString = strings.TrimSpace(rawString)
	selector := v.selector(options)
	if selector == nil {
		return nil, fmt.Errorf("expected selector not to be nil")
	}

	aValue, _, err := converter.Convert(rawString, v.codecType, false, "")
	if err != nil {
		return nil, err
	}

	aCriteria := NewCriteria(v.columns)
	evaluated, err := v.evaluator.Evaluate(nil, expand.WithParameters(aValue, nil), expand.WithDataUnit(aCriteria))

	if err != nil {
		return nil, err
	}

	return &parameter.Criteria{
		Query: evaluated.Expanded,
		Args:  evaluated.DataUnit.ParamsGroup,
	}, nil
}

func NewCriteria(columns parameter.ColumnsSource) *expand.DataUnit {
	return &expand.DataUnit{
		Columns: columns,
	}
}

func (v *VeltyCodec) selector(options []interface{}) parameter.Selector {
	var selector parameter.Selector
	for _, option := range options {
		switch actual := option.(type) {
		case parameter.Selector:
			selector = actual
		}
	}

	return selector
}

func (v *VeltyCodec) init() error {
	var err error
	v.evaluator, err = expand.NewEvaluator(nil, v.codecType, nil, v.template, func(name string, option ...xreflect.Option) (reflect.Type, error) {
		return nil, fmt.Errorf("unsupported type lookup at codec, yes")
	})

	return err
}
