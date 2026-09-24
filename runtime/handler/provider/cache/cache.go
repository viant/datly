// Package cache adapts explicitly named caches to native Bindly injection.
package cache

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
	xcache "github.com/viant/xdatly/cache"
)

const Kind = "cache"

// Registry borrows named caches supplied by its owner. It creates and closes none.
type Registry struct {
	backends map[string]xcache.Cache
}

var _ xcache.Provider = (*Registry)(nil)

// New snapshots the explicit registration map. Empty names and nil backends
// are invalid, including typed nil implementations.
func New(backends map[string]xcache.Cache) (*Registry, error) {
	registered := make(map[string]xcache.Cache, len(backends))
	for name, backend := range backends {
		if name == "" || nilValue(backend) {
			return nil, fmt.Errorf("invalid cache registration %q", name)
		}
		registered[name] = backend
	}
	return &Registry{backends: registered}, nil
}

// Cache returns only an exact registered name.
func (r *Registry) Cache(ctx context.Context, name string) (xcache.Cache, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || name == "" {
		return nil, fmt.Errorf("cache %q is not registered", name)
	}
	backend, ok := r.backends[name]
	if !ok {
		return nil, fmt.Errorf("cache %q is not registered", name)
	}
	return backend, nil
}

// Providers binds a supplied cache provider under kind=cache. Nil contributes
// no capability, so a required handler binding fails normally.
func Providers(value xcache.Provider) []locator.Provider {
	if nilValue(value) {
		return nil
	}
	return []locator.Provider{&static{value: value}}
}

type static struct{ value xcache.Provider }

func (*static) Kind() string                                { return Kind }
func (*static) Priority() int                               { return locator.PriorityDefault }
func (*static) DefaultCacheable() bool                      { return true }
func (p *static) Locate(*structology.State) locator.Locator { return p }

func (p *static) Value(_ context.Context, target reflect.Type, name string) (any, bool, error) {
	if name != "" {
		return nil, false, nil
	}
	contract := reflect.TypeFor[xcache.Provider]()
	if target != nil && !contract.AssignableTo(target) && !reflect.TypeOf(p.value).AssignableTo(target) {
		return nil, false, fmt.Errorf("%s binding must target %s, got %s", Kind, contract, target)
	}
	return p.value, true, nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	r := reflect.ValueOf(value)
	switch r.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return r.IsNil()
	}
	return false
}
