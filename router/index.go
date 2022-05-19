package router

import (
	"fmt"
	"github.com/viant/datly/view"
)

type (
	Index struct {
		SelectorPrefix map[string]string
		_viewsByPrefix map[string]int
		_viewsByName   map[string]int

		_viewDetails []*ViewDetails
	}

	ViewDetails struct {
		View *view.View
		Path string
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
	if i.SelectorPrefix == nil {
		i.SelectorPrefix = map[string]string{}
	}

	if i._viewsByPrefix == nil {
		i._viewsByPrefix = map[string]int{}
	}

	if i._viewsByName == nil {
		i._viewsByName = map[string]int{}
	}
}

func (i *Index) indexViews(view *view.View, path string) {
	i._viewsByName[view.Name] = len(i._viewDetails)
	i._viewDetails = append(i._viewDetails, &ViewDetails{
		View: view,
		Path: path,
	})

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
	for prefix, viewName := range i.SelectorPrefix {
		index, ok := i._viewsByName[viewName]
		if !ok {
			return fmt.Errorf("not found view %v with prefix %v", viewName, prefix)
		}

		i._viewsByPrefix[prefix] = index
	}

	return nil
}

func (i *Index) viewByName(name string) (*ViewDetails, bool) {
	index, ok := i._viewsByName[name]
	if !ok {
		return nil, false
	}

	return i._viewDetails[index], ok
}

func (i *Index) viewIndex(name string) (int, bool) {
	index, ok := i._viewsByName[name]
	return index, ok
}

func (i *Index) viewByPrefix(prefix string) (*view.View, bool) {
	index, ok := i._viewsByPrefix[prefix]
	if !ok {
		return nil, false
	}

	return i._viewDetails[index].View, ok
}
