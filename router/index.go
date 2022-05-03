package router

import (
	"fmt"
	"github.com/viant/datly/data"
)

type Index struct {
	ViewPrefix     map[string]string
	_viewsByPrefix map[string]*data.View
	_viewsByName   map[string]*data.View
	_views         []*data.View
}

func (i *Index) Init(route *Route) error {
	i.ensureIndexes()
	i.indexViews(route.View)

	if err := i.indexViewsByPrefix(); err != nil {
		return err
	}

	return nil
}

func (i *Index) ensureIndexes() {
	if i.ViewPrefix == nil {
		i.ViewPrefix = map[string]string{}
	}

	if i._viewsByPrefix == nil {
		i._viewsByPrefix = map[string]*data.View{}
	}

	if i._viewsByName == nil {
		i._viewsByName = map[string]*data.View{}
	}
}

func (i *Index) indexViews(view *data.View) {
	i._viewsByName[view.Name] = view
	i._views = append(i._views, view)

	for relationIndex := range view.With {
		i.indexViews(&view.With[relationIndex].Of.View)
	}
}

func (i *Index) indexViewsByPrefix() error {
	for prefix, viewName := range i.ViewPrefix {
		view, ok := i._viewsByName[viewName]
		if !ok {
			return fmt.Errorf("not found view %v with prefix %v", viewName, prefix)
		}

		i._viewsByPrefix[prefix] = view
	}

	return nil
}
