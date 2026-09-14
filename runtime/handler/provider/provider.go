// Package provider adapts runtime-owned values into Bindly providers. It does
// not own a registry; every provider is registered in the invocation scope.
package provider

import (
	"context"
	"reflect"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/locator/buildin"
	"github.com/viant/structology"
	xhandler "github.com/viant/xdatly/handler"
)

type provider struct {
	kind         string
	resolve      func(context.Context) (any, bool, error)
	resolveNamed func(context.Context, reflect.Type, string) (any, bool, error)
}

// Parameter resolves a derived param datapoint from the invocation input state.
func Parameter() locator.Provider {
	return buildin.Struct("param", "", locator.PriorityTransform)
}

// New adapts one invocation-local value resolver to a Bindly provider.
func New(kind xhandler.ValueKey, resolve func(context.Context) (any, bool, error)) locator.Provider {
	return &provider{kind: string(kind), resolve: resolve}
}

// Named adapts a provider whose values are selected by a binding source name.
func Named(kind string, resolve func(context.Context, reflect.Type, string) (any, bool, error)) locator.Provider {
	return &provider{kind: kind, resolveNamed: resolve}
}

// Static exposes one immutable invocation value under the supplied key.
func Static(kind xhandler.ValueKey, value any) locator.Provider {
	return New(kind, func(context.Context) (any, bool, error) {
		return value, hasValue(value), nil
	})
}

func (p *provider) Kind() string           { return p.kind }
func (p *provider) Priority() int          { return 0 }
func (p *provider) DefaultCacheable() bool { return true }
func (p *provider) Locate(*structology.State) locator.Locator {
	return &valueLocator{provider: p}
}

type valueLocator struct{ provider *provider }

func (l *valueLocator) Kind() string { return l.provider.kind }

func (l *valueLocator) Value(ctx context.Context, targetType reflect.Type, name string) (any, bool, error) {
	if l == nil || l.provider == nil {
		return nil, false, nil
	}
	if l.provider.resolveNamed != nil {
		return l.provider.resolveNamed(ctx, targetType, name)
	}
	if name != "" || l.provider.resolve == nil {
		return nil, false, nil
	}
	return l.provider.resolve(ctx)
}

func hasValue(value any) bool {
	if value == nil {
		return false
	}
	actual := reflect.ValueOf(value)
	switch actual.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !actual.IsNil()
	default:
		return true
	}
}
