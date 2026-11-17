package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
)

// orderDerivedBindings uses Bindly's existing priority ordering. Resolve input
// dependencies once during compilation; requests never rebuild this graph.
func orderDerivedBindings(bindings []bindly.BindingSpec) error {
	byPath := map[string]int{}
	for i, b := range bindings {
		byPath[b.Path] = i
	}
	states := make([]uint8, len(bindings))
	priorities := make([]int, len(bindings))
	var resolve func(int) (int, error)
	resolve = func(i int) (int, error) {
		if states[i] == 2 {
			return priorities[i], nil
		}
		if states[i] == 1 {
			return 0, fmt.Errorf("cyclic derived input binding at %s", bindings[i].Path)
		}
		states[i] = 1
		b := &bindings[i]
		priority := b.Priority
		if priority == 0 {
			switch b.Location.Kind {
			case "view", "data_view", "component":
				priority = locator.PriorityDependent
			case "input", "param":
				priority = locator.PriorityTransform
			}
		}
		if b.Location.Kind == "input" || b.Location.Kind == "param" {
			path := b.Location.In
			for path != "" {
				if parent, ok := byPath[path]; ok {
					value, err := resolve(parent)
					if err != nil {
						return 0, err
					}
					if value >= priority {
						priority = value + 1
						b.Priority = priority
					}
					break
				}
				cut := strings.LastIndexByte(path, '.')
				if cut < 0 {
					break
				}
				path = path[:cut]
			}
		}
		states[i] = 2
		priorities[i] = priority
		return priority, nil
	}
	for i := range bindings {
		if _, err := resolve(i); err != nil {
			return err
		}
	}
	return nil
}
