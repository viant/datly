package function

type Registry struct {
	byName map[string]Function
}

func (r *Registry) Register(name string, function Function) {
	r.byName[name] = function
}

var _registry = &Registry{byName: map[string]Function{}}

// Lookup returns dql function if defined
func Lookup(name string) Function {
	return _registry.byName[name]
}
