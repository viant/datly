package invocation

import (
	"context"
	"net/http"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
)

type httpHeadersKey struct{}

// WithHTTPHeaders captures this HTTP message's headers, not session state.
// Headers remain ordinary untrusted input; verified authorization is projected
// separately by RequestHeaders. No headers are added to MCP JSON-RPC messages.
func WithHTTPHeaders(ctx context.Context, headers http.Header) context.Context {
	snapshot := make(http.Header, len(headers))
	for name, values := range headers {
		key := http.CanonicalHeaderKey(name)
		snapshot[key] = append(snapshot[key], values...)
	}
	return context.WithValue(ctx, httpHeadersKey{}, snapshot)
}

// RequestHeaders returns a detached copy for native component binding.
// A raw transport credential must never replace the verified MCP credential.
func RequestHeaders(ctx context.Context) http.Header {
	result := make(http.Header)
	if ctx == nil {
		return result
	}
	if headers, ok := ctx.Value(httpHeadersKey{}).(http.Header); ok {
		result = headers.Clone()
	}
	result.Del("Authorization")
	if header := authorizationHeader(ctx); header != "" {
		result.Set("Authorization", header)
	}
	return result
}

// requestProviders reuses Bindly's provider composition so repeated transport
// headers retain their values and argument/body providers keep their ownership.
func requestProviders(ctx context.Context, scope *requestprovider.Scope) ([]locator.Provider, error) {
	providers := scope.Providers()
	headers := RequestHeaders(ctx)
	if len(headers) == 0 {
		return providers, nil
	}
	transport := requestprovider.NewValues(requestprovider.WithHeaders(headers)).Header()
	for index, provider := range providers {
		if provider.Kind() != requestprovider.HeaderKind {
			continue
		}
		combined, err := locator.ComposeProviders(requestprovider.HeaderKind,
			locator.ProviderLayer{Name: "transport", Provider: transport},
			locator.ProviderLayer{Name: "arguments", Provider: provider})
		if err != nil {
			return nil, err
		}
		providers[index] = combined
		return providers, nil
	}
	return append(providers, transport), nil
}
