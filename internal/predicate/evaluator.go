package predicate

import (
	"github.com/viant/velty"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type (
	Evaluator struct {
		template *predicate.Template
		args     []string
	}
)

var stringType = reflect.TypeOf("")

func (p *Evaluator) Expand(value interface{}) (*codec.Criteria, error) {
	planner := velty.New()
	if err := planner.DefineVariable("FilterValue", stringType); err != nil {
		return nil, err
	}
	for _, arg := range p.template.Args {
		if err := planner.DefineVariable(arg.Name, stringType); err != nil {
			return nil, err
		}
	}

	exec, newState, err := planner.Compile([]byte(p.template.Source))
	if err != nil {
		return nil, err
	}
	state := newState()
	for _, arg := range p.template.Args {
		if err := state.SetValue(arg.Name, p.args[arg.Position]); err != nil {
			return nil, err
		}
	}

	rValue := reflect.TypeOf(value)
	filterValue := "?"
	var values []interface{}
	if rValue.Kind() == reflect.Slice {
		ptr := xunsafe.AsPointer(value)
		xSlice := xunsafe.NewSlice(rValue)
		sliceLen := xSlice.Len(ptr)
		filterValue = strings.Repeat("?,", sliceLen)
		filterValue = filterValue[:len(filterValue)-1]
		for i := 0; i < sliceLen; i++ {
			values = append(values, xSlice.ValueAt(ptr, i))
		}
	} else {
		values = append(values, value)
	}
	if err := state.SetValue("FilterValue", filterValue); err != nil {
		return nil, err
	}
	if err := exec.Exec(state); err != nil {
		return nil, err
	}
	return &codec.Criteria{Query: state.Buffer.String(), Args: values}, nil
}

func NewEvaluator(template *predicate.Template, args ...string) *Evaluator {
	return &Evaluator{
		template: template,
		args:     args,
	}
}
