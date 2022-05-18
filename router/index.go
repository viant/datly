package router

import (
	"fmt"
	"github.com/viant/datly/view"
)

type (
	Index struct {
		ViewPrefix     map[string]string
		_viewsByPrefix map[string]*view.View
		_viewsByName   map[string]*viewDetails
		_views         []*view.View
	}

	viewDetails struct {
		view *view.View
		path string
	}
)

func (i *Index) Init(view *view.View, path string) error {
	i.ensureIndexes()
	i.indexViews(view, path)

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
		i._viewsByPrefix = map[string]*view.View{}
	}

	if i._viewsByName == nil {
		i._viewsByName = map[string]*viewDetails{}
	}
}

func (i *Index) indexViews(view *view.View, path string) {
	i._viewsByName[view.Name] = &viewDetails{
		view: view,
		path: path,
	}
	i._views = append(i._views, view)

	for relationIndex := range view.With {
		if path == "" {
			path = view.With[relationIndex].Holder
		} else {
			path += "." + view.With[relationIndex].Holder
		}

		i.indexViews(&view.With[relationIndex].Of.View, path)
	}
}

func (i *Index) indexViewsByPrefix() error {
	for prefix, viewName := range i.ViewPrefix {
		details, ok := i._viewsByName[viewName]
		if !ok {
			return fmt.Errorf("not found view %v with prefix %v", viewName, prefix)
		}

		i._viewsByPrefix[prefix] = details.view
	}

	return nil
}

func (i *Index) viewByName(name string) (*viewDetails, bool) {
	details, ok := i._viewsByName[name]
	return details, ok
}
