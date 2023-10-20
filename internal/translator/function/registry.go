package function

import "strings"

type Registry struct {
	byName map[string]Function
}

func (r *Registry) Register(function Function) {
	r.byName[key(function.Name())] = function
}

var _registry = &Registry{byName: map[string]Function{}}

// Lookup returns dql function if defined
func Lookup(name string) Function {
	return _registry.byName[key(name)]
}

func key(name string) string {
	fuzzyKey := strings.ReplaceAll(strings.ToLower(name), "_", "")
	return fuzzyKey
}
