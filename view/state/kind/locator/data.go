package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"os"
	"reflect"
)

type DataView struct {
	Views view.NamedViews
	ReadInto
}

func (p *DataView) Names() []string {
	return nil
}

func (p *DataView) Value(ctx context.Context, rType reflect.Type, name string) (interface{}, bool, error) {
	aView, ok := p.Views[name]
	if !ok {
		return nil, false, fmt.Errorf("failed to lookup view: %v", name)
	}
	if os.Getenv("DATLY_DEBUG_VIEW_LOCATOR") == "1" {
		fmt.Printf("[VIEW LOCATOR] name=%s schema=%v card=%s slice=%v\n", name, func() reflect.Type {
			if aView.Schema == nil {
				return nil
			}
			return aView.Schema.Type()
		}(), func() state.Cardinality {
			if aView.Schema == nil {
				return ""
			}
			return aView.Schema.Cardinality
		}(), func() reflect.Type {
			if aView.Schema == nil {
				return nil
			}
			return aView.Schema.SliceType()
		}())
	}
	sliceValue := reflect.New(aView.Schema.SliceType())
	destSlicePtr := sliceValue.Interface()
	err := p.ReadInto(ctx, destSlicePtr, aView)
	if err != nil {
		if os.Getenv("DATLY_DEBUG_VIEW_LOCATOR") == "1" {
			fmt.Printf("[VIEW LOCATOR] name=%s readIntoErr=%v dest=%T\n", name, err, destSlicePtr)
		}
		return nil, false, err
	}
	if os.Getenv("DATLY_DEBUG_VIEW_LOCATOR") == "1" {
		fmt.Printf("[VIEW LOCATOR] name=%s len=%d dest=%T\n", name, sliceValue.Elem().Len(), destSlicePtr)
	}

	if shouldReturnSingleValue(aView, rType) {
		switch sliceValue.Elem().Len() {
		case 0:
			return nil, true, nil
		case 1:
			return sliceValue.Elem().Index(0).Interface(), true, nil
		default:
			return nil, false, fmt.Errorf("multiple values found for view: %v, expected no mor than one", name)
		}
	}
	return sliceValue.Elem().Interface(), true, err
}

func shouldReturnSingleValue(aView *view.View, rType reflect.Type) bool {
	if aView != nil && aView.Schema != nil && aView.Schema.Cardinality == state.One {
		return true
	}
	if rType == nil {
		return false
	}
	for rType.Kind() == reflect.Interface {
		rType = rType.Elem()
	}
	switch rType.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return false
	default:
		return true
	}
}

func NewView(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.Views == nil {
		return nil, fmt.Errorf("views type was empty")
	}
	if options.ReadInto == nil {
		return nil, fmt.Errorf("ReadInto func was empty")
	}
	ret := &DataView{
		Views:    options.Views,
		ReadInto: options.ReadInto,
	}
	return ret, nil
}
