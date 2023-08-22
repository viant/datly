package router

import (
	"fmt"
	"github.com/viant/datly/view"
)

type (
	Index struct {
		Namespace map[string]string

		_nameToNamespace map[string]string
		_viewsByPrefix   map[string]int
		_viewsByName     map[string]int

		_viewDetails []*ViewDetails
	}

	ViewDetails struct {
		MainView bool
		View     *view.View
		Path     string
		Prefixes []string
	}
)

func (i *Index) Init(aView *view.View, path string) error {
	i.ensureIndexes()
	i.indexViews(aView, path, true)
	if err := i.indexViewsByPrefix(); err != nil {
		return err
	}
	i.addMainViewPrefixIfNeeded()
	return nil
}

func (i *Index) ensureIndexes() {
	if i.Namespace == nil {
		i.Namespace = map[string]string{}
	}

	if i._viewsByPrefix == nil {
		i._viewsByPrefix = map[string]int{}
	}

	if i._viewsByName == nil {
		i._viewsByName = map[string]int{}
	}

	if i._nameToNamespace == nil {
		i._nameToNamespace = map[string]string{}
	}
}

func (i *Index) indexViews(view *view.View, path string, isMain bool) {
	i._viewsByName[view.Name] = len(i._viewDetails)
	var prefixes []string
	if view.Selector.Namespace != "" {
		prefixes = append(prefixes, view.Selector.Namespace)
	}

	viewDetails := &ViewDetails{
		View:     view,
		Path:     path,
		Prefixes: prefixes,
		MainView: isMain,
	}

	i._viewDetails = append(i._viewDetails, viewDetails)

	for relationIndex := range view.With {
		aPath := path
		if aPath == "" {
			aPath = view.With[relationIndex].Holder
		} else {
			aPath += "." + view.With[relationIndex].Holder
		}

		i.indexViews(&view.With[relationIndex].Of.View, aPath, false)
	}

	for namespace, viewName := range i.Namespace {
		i._nameToNamespace[viewName] = namespace
	}
}

func (i *Index) ViewNamespace(aView *view.View) string {
	return i._nameToNamespace[aView.Name]
}

func (i *Index) indexViewsByPrefix() error {
	for prefix, viewName := range i.Namespace {
		index, ok := i._viewsByName[viewName]
		if !ok {
			return fmt.Errorf("not found view %v with prefix %v, %v", viewName, prefix, i._viewsByName)
		}

		i._viewsByPrefix[prefix] = index
		viewDetails := i._viewDetails[index]
		viewDetails.Prefixes = []string{prefix}
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

func (i *Index) prefixByView(aView *view.View) (string, bool) {
	name, ok := i._nameToNamespace[aView.Name]
	return name, ok
}

func (i *Index) addMainViewPrefixIfNeeded() {
	for _, detail := range i._viewDetails {
		if detail.MainView {
			if len(detail.Prefixes) == 0 {
				detail.Prefixes = append(detail.Prefixes, "")
				break
			}
		}
	}
}
