package runtime

import (
	"github.com/viant/datly/spec"
	"sort"
)

// Components returns detached canonical metadata without invoking handlers.
func (r *Runtime) Components() []*spec.Component {
	var result []*spec.Component
	for _, entry := range r.registered {
		result = append(result, entry.Component.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key.String() < result[j].Key.String() })
	return result
}
