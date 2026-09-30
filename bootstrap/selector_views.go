package bootstrap

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// ResolveQuerySelectorViews binds each parameter's query selector to the
// canonical name of the view it targets in the component's spec view graph.
// A unique SQL alias, view key, namespace or the component name resolves to
// that view's canonical name; an alias matching several views, or a canonical
// name shared by several views, is always rejected.
//
// With strict=false a target absent from the graph is left exactly as
// authored. Transcription uses this mode because nested views contributed by a
// linked Go output type do not exist until artifact bootstrap assembles them.
// With strict=true an unknown target is rejected; bootstrap uses this mode for
// handler-owned outputs, whose spec graph is final and never reaches the reader
// compiler's completed view index.
//
// Shared selector metadata is never mutated: a resolved binding is written as
// a fresh QuerySelectorBinding on the parameter.
func ResolveQuerySelectorViews(component *spec.Component, strict bool) error {
	if component == nil {
		return nil
	}
	type viewSet map[*spec.View]bool
	aliases := map[string]viewSet{}
	names := map[string]viewSet{}
	add := func(index map[string]viewSet, name string, view *spec.View) {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || view == nil {
			return
		}
		if index[name] == nil {
			index[name] = viewSet{}
		}
		index[name][view] = true
	}
	visited := viewSet{}
	var visit func(*spec.View)
	visit = func(view *spec.View) {
		if view == nil || visited[view] {
			return
		}
		visited[view] = true
		for _, name := range []string{view.Name, view.Key.Name} {
			add(names, name, view)
			add(aliases, name, view)
		}
		add(aliases, view.Namespace, view)
		for _, relation := range view.Relations {
			if relation != nil {
				visit(relation.View)
			}
		}
	}
	visit(component.RootView)
	add(aliases, component.Name, component.RootView)
	add(names, component.Name, component.RootView)
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || param.QuerySelector == nil {
			continue
		}
		selector := *param.QuerySelector
		target := strings.TrimSpace(selector.View)
		matches := aliases[strings.ToLower(target)]
		if target == "" && component.RootView != nil {
			matches = viewSet{component.RootView: true}
		}
		if len(matches) == 0 {
			if strict {
				return fmt.Errorf("query selector %s: unknown view %q", param.Name, selector.View)
			}
			// Deferred: the target may be a nested view linked from a Go output
			// type at artifact bootstrap, which validates it against the
			// completed graph.
			continue
		}
		if len(matches) != 1 {
			return fmt.Errorf("query selector %s: view alias %q is ambiguous", param.Name, selector.View)
		}
		for view := range matches {
			name := view.CanonicalName()
			// A unique SQL alias must still produce an unambiguous runtime name.
			if len(names[strings.ToLower(name)]) != 1 {
				return fmt.Errorf("query selector %s: canonical view name %q is ambiguous", param.Name, name)
			}
			selector.View = name
			if err := view.EnableQuerySelector(selector.Property); err != nil {
				return fmt.Errorf("query selector %s: %w", param.Name, err)
			}
		}
		param.QuerySelector = &selector
	}
	return nil
}
