package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	upstream "github.com/viant/mcp/server"
	"github.com/viant/mcp/server/auth"
)

// Source pins one immutable service to an invocation context. Repeated calls
// with that context must return the same service, even after publication.
type Source interface {
	Pin(context.Context) (context.Context, ServerService, error)
}

type sourceBinding struct {
	source Source
	broker *authBroker
}
type pinnedServiceKey struct{ binding *sourceBinding }

func (b *sourceBinding) pin(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("MCP request context is required")
	}
	if _, ok := b.service(ctx); ok {
		return ctx, nil
	}
	var prepared context.Context
	var service ServerService
	var err error
	if snapshot, ok := b.source.(interface {
		PinSnapshot(context.Context) (context.Context, ServerService, error)
	}); ok {
		prepared, service, err = snapshot.PinSnapshot(ctx)
	} else {
		prepared, service, err = b.source.Pin(ctx)
	}
	if err != nil {
		return nil, err
	}
	if prepared == nil || service == nil || service.Registry() == nil {
		return nil, fmt.Errorf("MCP source returned an incomplete snapshot")
	}
	return context.WithValue(prepared, pinnedServiceKey{b}, service), nil
}

func (b *sourceBinding) service(ctx context.Context) (ServerService, bool) {
	service, ok := ctx.Value(pinnedServiceKey{b}).(ServerService)
	return service, ok
}

func (c Config) sourceAuthorizer(policy *authorization.Policy, requireResourceAuthorization bool) (*auth.Service, error) {
	if c.BackendForFrontend == nil {
		return auth.New(&auth.Config{Policy: policy, AuthorizeResource: c.ResourceAuthorizer, RequireResourceAuthorization: requireResourceAuthorization})
	}
	return c.binding.broker.view(policy, requireResourceAuthorization)
}

func (c Config) sourceOptions() []upstream.Option {
	return []upstream.Option{
		upstream.WithRequestContext(c.binding.pin),
		upstream.WithJRPCAuthorizer(func(ctx context.Context, request *jsonrpc.Request, response *jsonrpc.Response) (*authorization.Token, error) {
			service, ok := c.binding.service(ctx)
			if !ok {
				return nil, fmt.Errorf("MCP invocation snapshot is missing")
			}
			policy := service.Authorization()
			if policy == nil {
				if c.BackendForFrontend != nil {
					return nil, fmt.Errorf("MCP OAuth broker requires an authorization policy")
				}
				return nil, nil
			}
			authorizer, err := c.sourceAuthorizer(policy, service.Registry().ImplementsSkills())
			if err != nil {
				return nil, err
			}
			return authorizer.EnsureAuthorized(ctx, request, response)
		}),
		upstream.WithAuthorizer(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx, err := c.binding.pin(r.Context())
				if err != nil {
					http.Error(w, "MCP service unavailable", http.StatusServiceUnavailable)
					return
				}
				r = r.WithContext(ctx)
				service, _ := c.binding.service(ctx)
				if policy := service.Authorization(); policy != nil {
					authorizer, err := c.sourceAuthorizer(policy, service.Registry().ImplementsSkills())
					if err != nil {
						http.Error(w, "MCP service unavailable", http.StatusServiceUnavailable)
						return
					}
					authorizer.Middleware(next).ServeHTTP(w, r)
					return
				}
				if c.BackendForFrontend != nil {
					http.Error(w, "MCP authorization unavailable", http.StatusServiceUnavailable)
					return
				}
				next.ServeHTTP(w, r)
			})
		}),
		upstream.WithProtectedResourcesHandler(func(w http.ResponseWriter, r *http.Request) {
			ctx, err := c.binding.pin(r.Context())
			if err != nil {
				http.Error(w, "MCP service unavailable", http.StatusServiceUnavailable)
				return
			}
			service, _ := c.binding.service(ctx)
			policy := service.Authorization()
			if policy == nil {
				if c.BackendForFrontend != nil {
					http.Error(w, "MCP authorization unavailable", http.StatusServiceUnavailable)
					return
				}
				http.NotFound(w, r)
				return
			}
			authorizer, err := c.sourceAuthorizer(policy, service.Registry().ImplementsSkills())
			if err != nil {
				http.Error(w, "MCP service unavailable", http.StatusServiceUnavailable)
				return
			}
			authorizer.ProtectedResourcesHandler(w, r.WithContext(ctx))
		}),
	}
}
