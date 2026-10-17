package compiler

import "fmt"

// regenerateCurrent consumes proof only after native role selection. An
// unrelated independent read cannot be removed by this generation path.
func (b *inputGeneration) regenerateCurrent(name string) error {
	p := b.names[name]
	if p == nil || !b.request.RegenerateCurrents[p.Identity()] {
		return nil
	}
	identity := b.request.ViewBindings[p.Identity()]
	if p.Source.Kind != "view" || identity == "" {
		return fmt.Errorf("generated Current %s has no canonical view binding", name)
	}
	for parameter, view := range b.request.ViewBindings {
		if parameter != p.Identity() && view == identity {
			return fmt.Errorf("generated Current %s shares view %s with %s", name, identity, parameter)
		}
	}
	count := 0
	for _, view := range b.request.Component.Views {
		if view != nil {
			candidate, err := view.Identity()
			if err != nil {
				return err
			}
			if candidate == identity {
				count++
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("generated Current %s requires one canonical view, found %d", name, count)
	}
	params := b.request.Component.Parameters[:0]
	for _, candidate := range b.request.Component.Parameters {
		if candidate != p {
			params = append(params, candidate)
		}
	}
	b.request.Component.Parameters = params
	views := b.request.Component.Views[:0]
	for _, view := range b.request.Component.Views {
		if view == nil {
			views = append(views, view)
			continue
		}
		candidate, _ := view.Identity()
		if candidate != identity {
			views = append(views, view)
		}
	}
	b.request.Component.Views = views
	delete(b.request.ViewBindings, p.Identity())
	delete(b.names, name)
	return nil
}
