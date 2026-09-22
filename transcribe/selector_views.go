package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// Resolve authored SQL aliases while the compiled view graph still carries
// them. Generated contracts bind selectors by canonical view name.
func resolveQuerySelectorViews(component *spec.Component) error {
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
		matches := aliases[strings.ToLower(strings.TrimSpace(selector.View))]
		if strings.TrimSpace(selector.View) == "" && component.RootView != nil {
			matches = viewSet{component.RootView: true}
		}
		if len(matches) == 0 {
			return fmt.Errorf("query selector %s: unknown view %q", param.Name, selector.View)
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
		}
		param.QuerySelector = &selector
	}
	return nil
}
