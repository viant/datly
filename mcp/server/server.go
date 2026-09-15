package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/viant/datly/internal/httpserver"
	"github.com/viant/jsonrpc/transport/server/stdio"
	upstream "github.com/viant/mcp/server"
	"github.com/viant/mcp/server/auth"
)

type Server struct {
	config   Config
	upstream *upstream.Server

	mu          sync.Mutex
	httpServer  *http.Server
	stdioCancel context.CancelFunc
}

func New(config Config) (*Server, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	newHandler, err := newHandler(config.Service, config.binding)
	if err != nil {
		return nil, err
	}
	options := []upstream.Option{
		upstream.WithNewHandler(newHandler),
		upstream.WithToolProtocolErrors(),
	}
	options = append(options, serverOptions(config)...)
	if config.Source != nil {
		options = append(options, config.sourceOptions()...)
	} else if policy := config.Service.Authorization(); policy != nil {
		authorizer, authErr := auth.New(&auth.Config{Policy: policy, AuthorizeResource: config.ResourceAuthorizer, RequireResourceAuthorization: config.Service.Registry().ImplementsSkills()})
		if authErr != nil {
			return nil, fmt.Errorf("create MCP authorizer: %w", authErr)
		}
		options = append(options, upstream.WithJRPCAuthorizer(authorizer.EnsureAuthorized))
		if config.Transport.Kind != TransportStdio {
			options = append(options,
				upstream.WithAuthorizer(authorizer.Middleware),
				upstream.WithProtectedResourcesHandler(authorizer.ProtectedResourcesHandler),
			)
		}
	}
	wrapped, err := upstream.New(options...)
	if err != nil {
		return nil, fmt.Errorf("create MCP transport server: %w", err)
	}
	if config.Transport.Kind == TransportStreamable {
		wrapped.UseStreamableHTTP(true)
	}
	return &Server{config: config, upstream: wrapped}, nil
}

func serverOptions(config Config) []upstream.Option {
	var result []upstream.Option
	if config.Implementation.Name != "" || config.Implementation.Version != "" {
		result = append(result, upstream.WithImplementation(config.Implementation))
	}
	if config.ProtocolVersion != "" {
		result = append(result, upstream.WithProtocolVersion(config.ProtocolVersion))
	}
	if config.LoggerName != "" {
		result = append(result, upstream.WithLoggerName(config.LoggerName))
	}
	transport := config.Transport
	if transport.CORS != nil {
		result = append(result, upstream.WithCORS(transport.CORS))
	}
	if transport.SSEURI != "" {
		result = append(result, upstream.WithSSEURI(transport.SSEURI))
	}
	if transport.SSEMessageURI != "" {
		result = append(result, upstream.WithSSEMessageURI(transport.SSEMessageURI))
	}
	if transport.StreamableURI != "" {
		result = append(result, upstream.WithStreamableURI(transport.StreamableURI))
	}
	if transport.RootRedirect {
		result = append(result, upstream.WithRootRedirect(true))
	}
	return result
}

func (s *Server) HTTP() (*http.Server, error) {
	if s == nil || s.upstream == nil {
		return nil, fmt.Errorf("MCP server is unavailable")
	}
	if s.config.Transport.Kind == TransportStdio {
		return nil, fmt.Errorf("MCP server uses stdio transport")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.httpServer == nil {
		s.httpServer = s.upstream.HTTP(context.Background(), s.config.Transport.Address)
		s.httpServer.ReadHeaderTimeout = s.config.Transport.ReadHeaderTimeout
		s.httpServer.IdleTimeout = s.config.Transport.IdleTimeout
		httpserver.Defaults(s.httpServer)
	}
	return s.httpServer, nil
}

func (s *Server) Stdio(ctx context.Context) (*stdio.Server, error) {
	if s == nil || s.upstream == nil {
		return nil, fmt.Errorf("MCP server is unavailable")
	}
	if s.config.Transport.Kind != TransportStdio {
		return nil, fmt.Errorf("MCP server uses %s transport", s.config.Transport.Kind)
	}
	return s.upstream.Stdio(ctx), nil
}
