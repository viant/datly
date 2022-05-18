package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
)

//Views represents views registry indexed by view name.
type Views map[string]*View

//Register registers view in registry using View name.
func (v *Views) Register(view *View) {
	if len(*v) == 0 {
		*v = make(map[string]*View)
	}
	keys := shared.KeysOf(view.Name, false)

	for _, key := range keys {
		(*v)[key] = view
	}
}

func (v *Views) merge(views *Views) {
	for key, _ := range *views {
		(*v)[key] = (*views)[key]
	}
}

//Lookup returns view by given name or error if view is not present.
func (v Views) Lookup(viewName string) (*View, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup view %v", viewName)
	}
	ret, ok := v[viewName]
	if !ok {

		return nil, fmt.Errorf("failed to lookup view %v", viewName)
	}
	return ret, nil
}

//ViewSlice wraps slice of Views
type ViewSlice []*View

//Index indexes ViewSlice by View.Name
func (v ViewSlice) Index() Views {
	result := Views(make(map[string]*View))
	for i := range v {
		result.Register(v[i])
	}
	return result
}

//Init initializes views.
func (v ViewSlice) Init(ctx context.Context, resource *Resource) error {
	for i := range v {
		if err := v[i].Init(ctx, resource); err != nil {
			return err
		}
	}

	return nil
}
