package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"reflect"
)

type DataView struct {
	Views view.NamedViews
	ReadInto
}

func (p *DataView) Names() []string {
	return nil
}

func (p *DataView) Value(ctx context.Context, name string) (interface{}, bool, error) {
	aView, ok := p.Views[name]
	if !ok {
		return nil, false, fmt.Errorf("failed to lookup view: %v", name)
	}
	sliceValue := reflect.New(aView.Schema.SliceType())
	destSlicePtr := sliceValue.Interface()
	err := p.ReadInto(ctx, destSlicePtr, aView)
	if err != nil {
		return nil, false, err
	}

	if aView.Schema.Cardinality == state.One {
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
