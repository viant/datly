package output

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xdatly/predicate"
	"reflect"
)

func (l *outputLocator) buildFilter(parameter *state.Parameter) (*structology.State, error) {
	filterType := structology.NewStateType(parameter.Schema.Type())
	filterState := filterType.NewState()
	if err := l.setFilterFields(filterState); err != nil {
		return nil, err
	}
	return filterState, nil
}

var (
	stringFilterType = reflect.TypeOf(&predicate.StringsFilter{})
	intFilterType    = reflect.TypeOf(&predicate.IntFilter{})
	boolFilterType   = reflect.TypeOf(&predicate.BoolFilter{})
)

func (l *outputLocator) setFilterFields(filterState *structology.State) error {
	var err error
	for i := range l.Output.Filters {
		filter := l.Output.Filters[i]
		value, _ := filterState.Selector(filter.Name)
		switch value.Type() {
		case intFilterType:
			aFilter := &predicate.IntFilter{}
			if aFilter.Include, err = asInts(filter.Include); err != nil {
				return err
			}
			if aFilter.Exclude, err = asInts(filter.Exclude); err != nil {
				return err
			}
			if err = filterState.SetValue(filter.Name, aFilter); err != nil {
				return err
			}
		case stringFilterType:
			aFilter := &predicate.StringsFilter{}
			if aFilter.Include, err = asStrings(filter.Include); err != nil {
				return err
			}
			if aFilter.Exclude, err = asStrings(filter.Exclude); err != nil {
				return err
			}
			if err = filterState.SetValue(filter.Name, aFilter); err != nil {
				return err
			}
		case boolFilterType:
			aFilter := &predicate.BoolFilter{}
			if aFilter.Include, err = asBool(filter.Include); err != nil {
				return err
			}
			if aFilter.Exclude, err = asBool(filter.Exclude); err != nil {
				return err
			}
			if err = filterState.SetValue(filter.Name, aFilter); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unuspported filter type: %s", value.Type().String())
		}
	}
	return nil
}

func asStrings(inputs []interface{}) ([]string, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	var output []string
	for _, input := range inputs {
		switch actual := input.(type) {
		case *string:
			if actual != nil {
				output = append(output, *actual)
			}
		case string:
			output = append(output, actual)
		default:
			return nil, fmt.Errorf("unable to case %T to []int", input)
		}
	}
	return output, nil
}

func asBool(inputs []interface{}) ([]bool, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	var output []bool
	for _, input := range inputs {
		switch actual := input.(type) {
		case *bool:
			if actual != nil {
				output = append(output, *actual)
			}
		case bool:
			output = append(output, actual)
		default:
			return nil, fmt.Errorf("unable to case %T to []int", input)
		}
	}
	return output, nil
}

func asInts(inputs []interface{}) ([]int, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	var output []int
	for _, input := range inputs {
		switch actual := input.(type) {
		case *int:
			if actual != nil {
				output = append(output, *actual)
			}
		case int:
			output = append(output, actual)
		default:
			return nil, fmt.Errorf("unable to case %T to []int", input)
		}
	}
	return output, nil
}
