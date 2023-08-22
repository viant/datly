package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/router/marshal"
)

type (
	IndexedViews struct {
		Views       []*NamespaceView
		byNamespace map[string]int
		byName      map[string]int
	}

	NamespaceView struct {
		View       *View
		Path       string
		Namespaces []string
	}
)

func (n *IndexedViews) ByNamespace(ns string) *NamespaceView {
	index, ok := n.byNamespace[ns]
	if !ok {
		return nil
	}
	return n.Views[index]
}

func (n *IndexedViews) ByName(ns string) *NamespaceView {
	index, ok := n.byName[ns]
	if !ok {
		return nil
	}
	return n.Views[index]
}

func (n *IndexedViews) indexView(aView *View, aPath string) {
	index := len(n.Views)
	selector := aView.Selector
	nsView := &NamespaceView{View: aView, Path: aPath}
	if aPath == "" {
		nsView.Namespaces = append(nsView.Namespaces, "")
	}
	if selector.Namespace != "" {
		nsView.Namespaces = append(nsView.Namespaces, selector.Namespace)
	}
	n.Views = append(n.Views, nsView)
	n.byName[aView.Name] = index
	for _, ns := range nsView.Namespaces {
		n.byNamespace[ns] = index
	}
	for _, aRelation := range aView.With {
		relPath := aPath
		if relPath == "" {
			relPath = aRelation.Holder
		} else {
			relPath += "." + aRelation.Holder
		}
		n.indexView(&aRelation.Of.View, relPath)
	}
}

// IndexViews indexes views
func IndexViews(aView *View) *IndexedViews {
	result := &IndexedViews{byNamespace: map[string]int{}, byName: map[string]int{}}
	result.indexView(aView, "")
	return result
}

// NamedViews represents views registry indexed by View name.
type NamedViews map[string]*View

// Register registers View in registry using View name.
func (v *NamedViews) Register(view *View) error {
	if len(*v) == 0 {
		*v = make(map[string]*View)
	}

	if _, ok := (*v)[view.Name]; ok {
		fmt.Printf("[WARN] View with %v name already exists in given resource", view.Name)
	}

	(*v)[view.Name] = view
	return nil
}

func (v *NamedViews) merge(views *NamedViews) {
	for key, _ := range *views {
		(*v)[key] = (*views)[key]
	}
}

// Lookup returns View by given name or error if View is not present.
func (v NamedViews) Lookup(viewName string) (*View, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup View %v", viewName)
	}
	ret, ok := v[viewName]
	if !ok {
		return nil, fmt.Errorf("failed to lookup View %v", viewName)
	}
	return ret, nil
}

// Views wraps slice of NamedViews
type Views []*View

// Index indexes Views by View.Name
func (v Views) Index() (NamedViews, error) {
	result := NamedViews(make(map[string]*View))
	for i := range v {
		if err := result.Register(v[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Init initializes views.
func (v Views) Init(ctx context.Context, resource *Resource, transforms marshal.TransformIndex) error {
	for i := range v {
		var options []ViewOption
		transform, ok := transforms[v[i].Name]
		if ok {
			options = append(options, WithTransforms(transform))
		}
		if err := v[i].Init(ctx, resource, options...); err != nil {
			return err
		}
	}

	return nil
}

func (v Views) EnsureResource(r *Resource) {
	for _, aView := range v {
		aView._resource = r
	}
}
