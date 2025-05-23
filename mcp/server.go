package mcp

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/http"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/mcp/extension"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client/auth/store"
	"github.com/viant/mcp/client/auth/transport"
	authserver "github.com/viant/mcp/server/auth"

	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/scy/auth/flow"
	"os"
	"path"

	"github.com/viant/mcp/server"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"golang.org/x/oauth2"
	"reflect"
	"strconv"
	"strings"
)

type Server struct {
	server   *server.Server // The underlying MCP server instance
	config   *gateway.ModelContextProtocol
	registry *serverproto.Registry
}

func (s *Server) init() error {

	var newImplementer = extension.New(s.registry)
	var options = []server.Option{
		server.WithNewImplementer(newImplementer),
		server.WithImplementation(schema.Implementation{"Datly", "0.1"}),
	}
	issuerURL := s.config.IssuerURL
	var oauth2Config *oauth2.Config
	var err error
	if s.config.AuthorizerMode != "" {

		if s.config.OAuth2ConfigURL != "" {
			if oauth2Config, err = loadAuthConfig(context.Background(), s.config); err != nil {

				return err
			}
			if issuerURL == "" && oauth2Config != nil {
				issuerURL, _ = url.Base(oauth2Config.Endpoint.AuthURL, http.SecureScheme)
			}
		}
		authPolicy := &authorization.Policy{
			ExcludeURI: "/sse",
			Global: &authorization.Authorization{
				ProtectedResourceMetadata: &meta.ProtectedResourceMetadata{
					Resource:             "https://datly.viantinc.com",
					AuthorizationServers: []string{issuerURL},
				},
				UseIdToken: true,
			},
		}

		var authService *authserver.Service
		switch s.config.AuthorizerMode {
		case "F":
			if oauth2Config == nil {
				return fmt.Errorf("Fallback mode requires OAuth2ConfigURL")
			}
			memStore := store.NewMemoryStore(store.WithClientConfig(oauth2Config))
			roundTripper, err := transport.New(
				transport.WithStore(memStore),
				transport.WithAuthFlow(flow.NewBrowserFlow()),
			)
			if err != nil {
				return fmt.Errorf("failed to create auth round tripper: %w", err)
			}
			authService, err = authserver.New(&authserver.Config{Policy: authPolicy})
			if err != nil {
				return fmt.Errorf("failed to create auth server: %w", err)
			}
			authService.FallBack = authserver.NewFallbackAuth(authService, roundTripper, roundTripper)
		default:
			header := flow.AuthorizationExchangeHeader
			if s.config.BFFExchangeHeader != "" {
				header = s.config.BFFExchangeHeader
			}
			authService, err = authserver.New(&authserver.Config{Policy: authPolicy,
				BackendForFrontend: &authserver.BackendForFrontend{
					Client:                      oauth2Config,
					AuthorizationExchangeHeader: header,
					RedirectURI:                 s.config.BFFRedirectURI,
				}})
			if err != nil {
				return fmt.Errorf("failed to create auth server: %w", err)
			}
		}
		options = append(options, server.WithAuthorizer(authService.Middleware))
		options = append(options, server.WithProtectedResourcesHandler(authService.ProtectedResourcesHandler))
	}

	srv, err := server.New(options...)
	if err != nil {
		return err
	}
	s.server = srv
	return nil
}

func (s *Server) ListenAndServe() error {

	if s.config.Port == nil {
		stdio := s.server.Stdio(context.Background())
		err := stdio.ListenAndServe()
		if err != nil {
			fmt.Printf("Server error: %v\n", err)
			return err
		}
	} else {
		httpServer := s.server.HTTP(context.Background(), ":"+strconv.Itoa(*s.config.Port))
		err := httpServer.ListenAndServe()
		if err != nil {
			// Handle error starting the SSE server
			fmt.Printf("Failed to start SSE server: %v\n", err)
			return err
		}
		defer httpServer.Shutdown(context.Background()) // Ensure the server shuts down gracefully
	}
	return nil
}

func loadAuthConfig(ctx context.Context, mcpOption *gateway.ModelContextProtocol) (*oauth2.Config, error) {
	if authClientURL := mcpOption.OAuth2ConfigURL; authClientURL != "" {
		if url.IsRelative(authClientURL) {
			fs := afs.New()
			candidateLocation := path.Join(os.Getenv("HOME"), ".secret", authClientURL)
			if ok, _ := fs.Exists(ctx, candidateLocation); ok {
				authClientURL = candidateLocation
			}
		}

		keyURL := "blowfish://default"
		if index := strings.Index(mcpOption.OAuth2ConfigURL, "|"); index != -1 {
			keyURL = authClientURL[index+1:]
			authClientURL = authClientURL[:index]
		}
		resource := scy.NewResource(reflect.TypeOf(&cred.Oauth2Config{}), authClientURL, keyURL)
		secrets := scy.New()
		secret, err := secrets.Load(ctx, resource)
		if err != nil {
			return nil, err
		}
		if secret == nil {
			return nil, fmt.Errorf("secret was nil")
		}
		oAuth2Config, ok := secret.Target.(*cred.Oauth2Config)
		if !ok {
			return nil, fmt.Errorf("secret was not of type *cred.Oauth2Config")
		}
		oAuth2Config.Endpoint.AuthStyle = oauth2.AuthStyleInHeader
		return &oAuth2Config.Config, nil
	}
	return nil, nil
}

func NewServer(registry *serverproto.Registry, config *gateway.ModelContextProtocol) (*Server, error) {
	if registry == nil {
		return nil, nil
	}
	s := &Server{
		config:   config,
		registry: registry,
	}

	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}
