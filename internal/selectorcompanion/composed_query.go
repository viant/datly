package selectorcompanion

import (
	"reflect"

	"github.com/viant/bindly/locator"
)

// composedQuerySource is minted only by the engine after its provider-layer
// validation. It retains the composed locator unchanged: child overrides,
// authoritative absence, errors, and inherited request state keep their native
// lookup semantics. Admission never reads a value before reader validation.
type composedQuerySource struct{ locator.Provider }

func (p *composedQuerySource) DefaultCacheable() bool {
	policy, ok := p.Provider.(locator.CachePolicy)
	return ok && policy.DefaultCacheable()
}

// CarryQueryAuthority retains an inherited native request source across an
// engine-owned composition. Unknown protocol sources do not gain authority.
func CarryQueryAuthority(composed, inherited locator.Provider) locator.Provider {
	if nilQueryProvider(composed) || composed.Kind() != "query" || !nativeQueryAuthority(inherited) {
		return composed
	}
	return &composedQuerySource{Provider: composed}
}

func nativeQueryAuthority(provider locator.Provider) bool {
	if provider == nil {
		return false
	}
	if composed, ok := provider.(*composedQuerySource); ok {
		return composed != nil && !nilQueryProvider(composed.Provider) && composed.Kind() == "query"
	}
	return reflect.TypeOf(provider) == nativeQueryProviderType && !reflect.ValueOf(provider).IsNil() && provider.Kind() == "query"
}

func nilQueryProvider(provider locator.Provider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
