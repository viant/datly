package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"sync/atomic"
)

type Repeated struct {
	ParameterLookup
	Parameters state.NamedParameters
}

type entry struct {
	value interface{}
	has   bool
	err   error
}

func (p *Repeated) Names() []string {
	return nil
}

func (p *Repeated) Value(ctx context.Context, names string) (interface{}, bool, error) {
	parameter := p.matchByLocation(names)
	if parameter == nil {
		return nil, false, fmt.Errorf("failed to match parameter by location: %v", names)
	}
	temp, hasCount := p.getRepeatedItems(ctx, parameter)
	if hasCount == 0 {
		return nil, false, nil
	}
	var values = make([]interface{}, 0, int(hasCount))
	for i := range temp {
		anEntry := &temp[i]
		if err := anEntry.err; err != nil {
			return nil, false, err
		}
		if !anEntry.has {
			continue
		}
		values = append(values, anEntry.value)
	}
	xSlice := parameter.Schema.Slice()
	slicePtrValue := reflect.New(xSlice.Type)
	sliceValue := reflect.MakeSlice(xSlice.Type, 0, int(hasCount))
	slicePtrValue.Elem().Set(sliceValue)
	ptr := xunsafe.ValuePointer(&slicePtrValue)
	appender := xSlice.Appender(ptr)
	appender.Append(values...)
	return slicePtrValue.Elem().Interface(), true, nil
}

func (p *Repeated) getRepeatedItems(ctx context.Context, parameter *state.Parameter) ([]entry, int32) {
	var temp = make([]entry, len(parameter.Repeated))
	wg := sync.WaitGroup{}
	hasCount := int32(0)
	for i := range parameter.Repeated {
		wg.Add(1)
		go func(index int, item *state.Parameter) {
			defer wg.Done()
			anEntry := &temp[index]
			if anEntry.value, anEntry.has, anEntry.err = p.ParameterLookup(ctx, item); anEntry.has {
				atomic.AddInt32(&hasCount, 1)
			}
		}(i, parameter.Repeated[i])
	}
	wg.Wait()
	return temp, hasCount
}

func (p *Repeated) matchByLocation(names string) *state.Parameter {
	var parameter *state.Parameter
	for _, candidate := range p.Parameters {
		if candidate.In.Kind == state.KindRepeated && candidate.In.Name == names {
			parameter = candidate
		}
	}
	return parameter
}

// NewRepeated returns parameter locator
func NewRepeated(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.ParameterLookup == nil {
		return nil, fmt.Errorf("parameterLookup was empty")
	}
	if options.InputParameters == nil {
		return nil, fmt.Errorf("parameters was empty")
	}
	return &Repeated{
		ParameterLookup: options.ParameterLookup,
		Parameters:      options.InputParameters,
	}, nil
}
