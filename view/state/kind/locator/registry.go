package locator

import "github.com/viant/datly/view/state"

type registry struct {
	registry map[state.Kind]NewLocator
}

var _registry = newRegistry()

func Register(kind state.Kind, newLocator NewLocator) {
	_registry.Register(kind, newLocator)
}

func Lookup(kind state.Kind) NewLocator {
	return _registry.Lookup(kind)
}

func (r *registry) Register(kind state.Kind, newLocator NewLocator) {
	r.registry[kind] = newLocator
}

func (r *registry) Lookup(kind state.Kind) NewLocator {
	return r.registry[kind]
}

func newRegistry() *registry {
	return &registry{registry: make(map[state.Kind]NewLocator)}
}
