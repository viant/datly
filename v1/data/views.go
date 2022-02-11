package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/v1/config"
)

type Views map[string]*View

func (v *Views) Register(view *View) {
	if len(*v) == 0 {
		*v = make(map[string]*View)
	}
	keys := KeysOf(view.Name, false)

	for _, key := range keys {
		(*v)[key] = view
	}
}

func (v Views) Lookup(ref string) (*View, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup view %v", ref)
	}
	ret, ok := v[ref]
	if !ok {
		return nil, fmt.Errorf("failed to lookup view %v", ref)
	}
	return ret, nil
}

type ViewSlice []*View

func (v ViewSlice) Index() Views {
	result := Views(make(map[string]*View))
	for i := range v {
		result.Register(v[i])
	}
	return result
}

func (v ViewSlice) Init(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	for i := range v {
		if err := v[i].Init(ctx, views, connectors, types); err != nil {
			return err
		}
	}
	return nil
}
