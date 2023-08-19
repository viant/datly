package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/template/expand"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
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
		columns   codec.ColumnsSource
	}
)

func (v *VeltyCodec) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&codec.Criteria{}), nil
}

func (v *VeltyCriteriaFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	opts := NewOptions(codec.NewOptions(options))
	columnsIndex := opts.ColumnsSource
	vCodec := &VeltyCodec{
		template:  codecConfig.Body,
		codecType: codecConfig.InputType,
		columns:   columnsIndex,
	}
	if err := vCodec.init(); err != nil {
		return nil, err
	}

	return vCodec, nil
}

func (v *VeltyCodec) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to get string but got %T", raw)
	}

	opts := NewOptions(codec.NewOptions(options))
	rawString = strings.TrimSpace(rawString)
	selector := opts.Selector
	if selector == nil {
		return nil, fmt.Errorf("expected selector not to be nil")
	}

	aValue, _, err := converter.Convert(rawString, v.codecType, false, "")
	if err != nil {
		return nil, err
	}
	stateType := structology.NewStateType(reflect.TypeOf(aValue))
	state := stateType.WithValue(aValue)
	aCriteria := NewCriteria(v.columns)
	evaluated, err := v.evaluator.Evaluate(nil, expand.WithParameterState(state), expand.WithDataUnit(aCriteria))

	if err != nil {
		return nil, err
	}

	return &codec.Criteria{
		Query: evaluated.Expanded,
		Args:  evaluated.DataUnit.ParamsGroup,
	}, nil
}

func NewCriteria(columns codec.ColumnsSource) *expand.DataUnit {
	return &expand.DataUnit{
		Columns: columns,
	}
}

func (v *VeltyCodec) selector(options []interface{}) codec.Selector {
	var selector codec.Selector
	for _, option := range options {
		switch actual := option.(type) {
		case codec.Selector:
			selector = actual
		}
	}

	return selector
}

func (v *VeltyCodec) init() error {
	var err error
	stateType := structology.NewStateType(v.codecType)
	v.evaluator, err = expand.NewEvaluator(v.template, expand.WithStateType(stateType), expand.WithTypeLookup(v.lookupType))

	return err
}

func (v *VeltyCodec) lookupType(name string, option ...xreflect.Option) (reflect.Type, error) {
	return nil, fmt.Errorf("unsupported type lookup at codec")
}
