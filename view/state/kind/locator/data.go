package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"reflect"
)

type DataView struct {
	Views        view.NamedViews
	Parameters   state.NamedParameters
	ReadViewData func(ctx context.Context, dest interface{}, aView *view.View) error
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
