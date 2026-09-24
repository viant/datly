// Package clients is the composition-root wiring for outbound client
// capabilities. It adapts the public xdatly HTTP and MCP provider contracts to
// native Bindly input binding and owns the lifecycle of Datly's default
// implementations. Handlers declare
//
//	HTTP xhttp.Provider `bind:"kind=http_client,required"`
//	MCP  xmcp.Provider  `bind:"kind=mcp_client,required"`
//
// and receive whichever provider the runtime composition registered. Nothing
// here inspects request values; a provider is either registered or the
// binding fails.
package clients

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/locator"
	iclient "github.com/viant/datly/internal/client"
	"github.com/viant/structology"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

const (
	// HTTPKind is the Bindly source kind that resolves xhttp.Provider.
	HTTPKind = "http_client"
	// MCPKind is the Bindly source kind that resolves xmcp.Provider.
	MCPKind = "mcp_client"
)

// Providers adapts explicit provider implementations to Bindly providers. A
// nil implementation registers nothing for its kind, so a handler that
// requires that capability fails its binding instead of receiving a hidden
// default.
func Providers(http xhttp.Provider, mcp xmcp.Provider) []locator.Provider {
	var result []locator.Provider
	if !nilProvider(http) {
		result = append(result, &static{kind: HTTPKind, value: http, target: reflect.TypeFor[xhttp.Provider]()})
	}
	if !nilProvider(mcp) {
		result = append(result, &static{kind: MCPKind, value: mcp, target: reflect.TypeFor[xmcp.Provider]()})
	}
	return result
}

func nilProvider(value any) bool {
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

// Registry owns Datly's default HTTP and MCP client implementations. The
// composition root that creates it must Close it at shutdown; handlers only
// borrow the providers it exposes.
type Registry struct {
	registry *iclient.Registry
}

// New creates an empty default registry.
func New() *Registry {
	return &Registry{registry: iclient.NewRegistry()}
}

// HTTP returns the default HTTP provider, or nil for a nil registry.
func (r *Registry) HTTP() xhttp.Provider {
	if r == nil || r.registry == nil {
		return nil
	}
	return r.registry.HTTP()
}

// MCP returns the default MCP provider, or nil for a nil registry.
func (r *Registry) MCP() xmcp.Provider {
	if r == nil || r.registry == nil {
		return nil
	}
	return r.registry.MCP()
}

// Providers returns the Bindly providers for both default capabilities. A nil
// registry contributes nothing, so dependent bindings fail closed.
func (r *Registry) Providers() []locator.Provider {
	return Providers(r.HTTP(), r.MCP())
}

// Clients reports the number of distinct configured clients currently held.
func (r *Registry) Clients() int {
	if r == nil || r.registry == nil {
		return 0
	}
	return r.registry.Size()
}

// Close releases every client the registry created and refuses further
// resolution.
func (r *Registry) Close() error {
	if r == nil || r.registry == nil {
		return nil
	}
	return r.registry.Close()
}

// static is a name-less, cacheable provider for one immutable interface value.
type static struct {
	kind   string
	value  any
	target reflect.Type
}

func (p *static) Kind() string                              { return p.kind }
func (p *static) Priority() int                             { return locator.PriorityDefault }
func (p *static) DefaultCacheable() bool                    { return true }
func (p *static) Locate(*structology.State) locator.Locator { return p }

func (p *static) Value(_ context.Context, targetType reflect.Type, name string) (any, bool, error) {
	if name != "" {
		return nil, false, nil
	}
	if targetType != nil && !p.target.AssignableTo(targetType) && !reflect.TypeOf(p.value).AssignableTo(targetType) {
		return nil, false, fmt.Errorf("%s binding must target %s, got %s", p.kind, p.target, targetType)
	}
	return p.value, true, nil
}
