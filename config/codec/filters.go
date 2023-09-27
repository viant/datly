package codec

import (
	"context"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"reflect"
	"strconv"
	"sync"
)

const (
	KeyFilters = "AsFilters"
)

type (
	FiltersRegistry struct {
		sync.Mutex
		registry map[reflect.Type]*structology.StateType
	}

	Filters struct {
		registry *FiltersRegistry
	}
)

func (e *FiltersRegistry) get(key reflect.Type) *structology.StateType {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	ret, ok := e.registry[key]
	if !ok {
		ret = structology.NewStateType(key)
		e.registry[key] = ret
	}
	return ret
}

func (e *FiltersRegistry) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if len(e.registry) == 0 {
		e.registry = map[reflect.Type]*structology.StateType{}
	}

	if err := ValidateMinArgs(codecConfig, KeyFilters, 0); err != nil {
		return nil, err
	}
	ret := &Filters{registry: e}
	return ret, ret.init()
}

var namedFilterTypes = reflect.TypeOf(predicate.NamedFilters{})

func (e *Filters) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return namedFilterTypes, nil
}

func (e *Filters) init() error {
	return nil
}

func (e *Filters) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	if raw == nil {
		return nil, nil
	}
	aStateType := e.registry.get(reflect.TypeOf(raw))
	aState := aStateType.WithValue(raw)
	var result predicate.NamedFilters
	for _, selector := range aStateType.RootSelectors() {
		value := selector.Value(aState.Pointer())
		switch actual := value.(type) {
		case *predicate.IntFilter:
			aFilter := &predicate.NamedFilter{Name: selector.Name()}
			for _, item := range actual.Include {
				aFilter.Include = append(aFilter.Include, strconv.Itoa(item))
			}
			for _, item := range actual.Exclude {
				aFilter.Exclude = append(aFilter.Exclude, strconv.Itoa(item))
			}
			result = append(result, aFilter)
		case *predicate.StringsFilter:
			result = append(result, &predicate.NamedFilter{Name: selector.Name(), Include: actual.Include, Exclude: actual.Exclude})
		case *predicate.BoolFilter:
			aFilter := &predicate.NamedFilter{Name: selector.Name()}
			for _, item := range actual.Include {
				aFilter.Include = append(aFilter.Include, strconv.FormatBool(item))
			}
			for _, item := range actual.Exclude {
				aFilter.Exclude = append(aFilter.Exclude, strconv.FormatBool(item))
			}
			result = append(result, aFilter)
		}
	}
	return result, nil
}
