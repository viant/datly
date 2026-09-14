package reader

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

// ViewIndex is the immutable registration-time index of prepared views.
// Aliases may be duplicated; ambiguity is reported only when that alias is
// selected, so unrelated repeated child names remain valid.
type ViewIndex struct {
	root    *data.View
	aliases map[string][]*data.View
}

func NewViewIndex(component *spec.Component, root *data.View) *ViewIndex {
	index := &ViewIndex{root: root, aliases: map[string][]*data.View{}}
	visited := map[*data.View]bool{}
	var visit func(*data.View)
	visit = func(view *data.View) {
		if view == nil || visited[view] {
			return
		}
		visited[view] = true
		index.add(view.Spec.Name, view)
		index.add(view.Spec.Key.Name, view)
		for _, relation := range view.Relations {
			if relation != nil && relation.Of != nil {
				visit(relation.Of.View)
			}
		}
	}
	visit(root)
	if component != nil {
		index.add(component.Name, root)
	}
	return index
}

func (i *ViewIndex) Resolve(alias string) (*data.View, error) {
	if i == nil {
		return nil, fmt.Errorf("prepared view index is required")
	}
	key := normalizeViewAlias(alias)
	if key == "" {
		if i.root == nil {
			return nil, fmt.Errorf("root view is not registered")
		}
		return i.root, nil
	}
	matches := i.aliases[key]
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("unknown view %q", alias)
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("view alias %q is ambiguous", alias)
	}
}

func (i *ViewIndex) add(alias string, view *data.View) {
	key := normalizeViewAlias(alias)
	if key == "" || view == nil {
		return
	}
	for _, existing := range i.aliases[key] {
		if existing == view {
			return
		}
	}
	i.aliases[key] = append(i.aliases[key], view)
}

func normalizeViewAlias(alias string) string {
	return strings.ToLower(strings.TrimSpace(alias))
}
