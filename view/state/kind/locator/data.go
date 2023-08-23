package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state/kind"
	"reflect"
)

type DataView struct {
	Views view.NamedViews
	ReadViewData
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
	err := p.ReadViewData(ctx, destSlicePtr, aView)
	if err != nil {
		return nil, false, err
	}
	return sliceValue.Elem().Interface(), true, err
}

func NewDataView(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.Views == nil {
		return nil, fmt.Errorf("views type was empty")
	}
	if options.ReadViewData == nil {
		return nil, fmt.Errorf("ReadViewData func was empty")
	}
	ret := &DataView{
		Views:        options.Views,
		ReadViewData: options.ReadViewData,
	}
	return ret, nil
}
