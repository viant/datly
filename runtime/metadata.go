package runtime

import (
	"github.com/viant/datly/spec"
	"sort"
)

// Components returns detached canonical metadata without invoking handlers.
func (r *Runtime) Components() []*spec.Component {
	var result []*spec.Component
	for _, component := range r.metadata {
		result = append(result, component.Clone())
	}
	r.relatedMetadata.Range(func(_, value any) bool {
		if component, ok := value.(*spec.Component); ok && component != nil {
			result = append(result, component.Clone())
		}
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].Key.String() < result[j].Key.String() })
	return result
}
