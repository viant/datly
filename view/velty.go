package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/template/expand"
	"reflect"
	"strings"
)

type VeltyCodec struct {
	template  string
	codecType reflect.Type
	evaluator *expand.Evaluator
	columns   ColumnIndex
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

	selector.Criteria = evaluated.Expanded
	selector.Placeholders = evaluated.DataUnit.ParamsGroup
	return nil, nil
}

func NewCriteria(columns ColumnIndex) *expand.DataUnit {
	return &expand.DataUnit{
		Columns: columns,
	}
}

func extractActualId(name string) string {
	if len(name) == 0 {
		return name
	}

	name = name[1:] // skip $
	if len(name) > 0 && byte(name[0]) == '{' && byte(name[len(name)-1]) == '}' {
		name = name[1 : len(name)-1]
	}

	return name
}

func (v *VeltyCodec) selector(options []interface{}) *Selector {
	var selector *Selector
	for _, option := range options {
		switch actual := option.(type) {
		case *Selector:
			selector = actual
		}
	}

	return selector
}

func NewVeltyCodec(template string, paramType reflect.Type, view *View) (*VeltyCodec, error) {
	var columns ColumnIndex
	if view != nil {
		columns = view._columns
	}

	codec := &VeltyCodec{
		template:  template,
		codecType: paramType,
		columns:   columns,
	}

	if err := codec.init(); err != nil {
		return nil, err
	}

	return codec, nil
}

func (v *VeltyCodec) init() error {
	var err error
	v.evaluator, err = expand.NewEvaluator(nil, v.codecType, nil, v.template, func(packagePath, packageIdentifier, typeName string) (reflect.Type, error) {
		return nil, fmt.Errorf("unsupported type lookup at codec, yes")
	})

	return err
}
