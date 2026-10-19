package server

import (
	"context"
	"net/http"
	"sync"

	"github.com/viant/jsonrpc"
	streamauth "github.com/viant/jsonrpc/transport/server/auth"
	"github.com/viant/mcp-protocol/authorization"
	upstream "github.com/viant/mcp/server"
	"github.com/viant/mcp/server/auth"
)

// authBroker retains the upstream OAuth exchange state for one MCP listener.
// Its grant store is shared explicitly with that listener's HTTP transport.
type authBroker struct {
	once               sync.Once
	seed               *auth.Service
	err                error
	backendForFrontend *auth.BackendForFrontend
	resourceAuthorizer auth.ResourceAuthorizer
	grantStore         streamauth.Store
}

// view binds one immutable, pinned policy without mutating shared broker state.
// Upstream's pointer-backed PKCE and token stores remain shared across views.
func (b *authBroker) view(policy *authorization.Policy, requireResourceAuthorization bool) (*auth.Service, error) {
	b.once.Do(func() {
		b.seed, b.err = auth.New(&auth.Config{BackendForFrontend: b.backendForFrontend, BFFGrantStore: b.grantStore})
	})
	if b.err != nil {
		return nil, b.err
	}
	view := *b.seed
	config := *b.seed.Config
	config.Policy = policy
	config.AuthorizeResource = b.resourceAuthorizer
	config.RequireResourceAuthorization = requireResourceAuthorization
	view.Config = &config
	return &view, nil
}

func staticBFFOptions(config Config, policy *authorization.Policy) []upstream.Option {
	broker := &authBroker{backendForFrontend: config.BackendForFrontend, resourceAuthorizer: config.ResourceAuthorizer, grantStore: config.bffGrantStore}
	authorizer := func() (*auth.Service, error) {
		return broker.view(policy, config.Service.Registry().ImplementsSkills())
	}
	result := []upstream.Option{upstream.WithJRPCAuthorizer(func(ctx context.Context, request *jsonrpc.Request, response *jsonrpc.Response) (*authorization.Token, error) {
		service, err := authorizer()
		if err != nil {
			return nil, err
		}
		return service.EnsureAuthorized(ctx, request, response)
	})}
	if config.Transport.Kind == TransportStdio {
		return result
	}
	return append(result,
		upstream.WithAuthorizer(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				service, err := authorizer()
				if err != nil {
					http.Error(w, "MCP authorization unavailable", http.StatusServiceUnavailable)
					return
				}
				service.Middleware(forwardCatalogBearer(config.Service, next)).ServeHTTP(w, r)
			})
		}),
		upstream.WithProtectedResourcesHandler(func(w http.ResponseWriter, r *http.Request) {
			service, err := authorizer()
			if err != nil {
				http.Error(w, "MCP authorization unavailable", http.StatusServiceUnavailable)
				return
			}
			service.ProtectedResourcesHandler(w, r)
		}),
	)
}
