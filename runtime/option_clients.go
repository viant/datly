package runtime

import (
	"fmt"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/handler/provider/clients"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

// WithClientProviders replaces the runtime's default outbound client
// providers with explicit implementations of the public xdatly contracts. The
// caller owns their lifecycle; the runtime neither closes nor substitutes
// them. A nil implementation leaves that capability unavailable, so a handler
// requiring it fails its binding instead of receiving a hidden default.
func WithClientProviders(http xhttp.Provider, mcp xmcp.Provider) Option {
	return func(options *options) error {
		providers := clients.Providers(http, mcp)
		if len(providers) == 0 {
			return fmt.Errorf("client providers: at least one of http or mcp is required")
		}
		options.clientProviders = providers
		options.clientProvidersConfigured = true
		return nil
	}
}

// withDefaultClientProviders appends the runtime-level client providers for
// every kind the component registration did not supply itself. Component
// providers of the same kind therefore replace the runtime default
// explicitly, per component.
func withDefaultClientProviders(component []locator.Provider, defaults []locator.Provider) []locator.Provider {
	if len(defaults) == 0 {
		return component
	}
	present := make(map[string]bool, len(component))
	for _, provider := range component {
		if provider != nil {
			present[provider.Kind()] = true
		}
	}
	result := component
	for _, provider := range defaults {
		if provider == nil || present[provider.Kind()] {
			continue
		}
		result = append(result, provider)
	}
	return result
}

// ClientProviders reports the runtime-level client providers applied to
// components that do not register their own.
func (r *Runtime) ClientProviders() []locator.Provider {
	if r == nil {
		return nil
	}
	return append([]locator.Provider(nil), r.clientProviders...)
}
