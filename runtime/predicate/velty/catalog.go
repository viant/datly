package velty

import (
	"sort"
)

// Names returns the predicate names available to compilation. The returned
// slice is detached and sorted for stable authoring metadata and validation.
func Names() []string {
	registry := predicateRegistry()
	result := make([]string, 0, len(registry))
	for name := range registry {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}
