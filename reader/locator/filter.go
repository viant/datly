package locator

import (
	"encoding/json"
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

func (l *outputLocator) setFilterFields(filterState *structology.State) error {
	var err error
	for i := range l.Output.Filters {
		filter := l.Output.Filters[i]
		value, _ := filterState.Value(filter.Name)
		switch value.(type) {
		case *predicate.IntFilter:
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
		case *predicate.StringsFilter:
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
		case *predicate.BoolFilter:
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
			return fmt.Errorf("unuspported filter type: %T", value)
		}
	}
	return nil
}

func asStrings(input interface{}) ([]string, error) {
	if input == nil {
		return nil, nil
	}
	var output []string
	switch actual := input.(type) {
	case *string:
		if actual != nil {
			output = []string{*actual}
		}
	case string:
		output = []string{actual}
	case []string:
		output = actual
	default:
		rType := reflect.TypeOf(input)
		if rType.Kind() == reflect.Slice {
			rType = rType.Elem()
		}
		if rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
		if rType.Kind() == reflect.Struct {
			if data, err := json.Marshal(input); err == nil {
				output = []string{string(data)}
			}
		}
		if output == nil {
			output = []string{fmt.Sprintf("%v", input)}
		}
	}
	return output, nil
}

func asBool(input interface{}) ([]bool, error) {
	if input == nil {
		return nil, nil
	}
	var output []bool
	switch actual := input.(type) {
	case *bool:
		if actual != nil {
			output = []bool{*actual}
		}
	case bool:
		output = []bool{actual}
	case []bool:
		output = actual
	default:
		return nil, fmt.Errorf("unable to case %T to []bool", input)
	}
	return output, nil
}

func asInts(input interface{}) ([]int, error) {
	if input == nil {
		return nil, nil
	}
	var output []int
	switch actual := input.(type) {
	case *int:
		if actual != nil {
			output = []int{*actual}
		}
	case int:
		output = []int{actual}
	case []int:
		output = actual
	default:
		return nil, fmt.Errorf("unable to case %T to []int", input)
	}
	return output, nil
}
