package mcp

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/http"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/mcp/extension"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client/auth/flow"
	"github.com/viant/mcp/client/auth/store"
	"github.com/viant/mcp/client/auth/transport"
	authserver "github.com/viant/mcp/server/auth"
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
	server    *server.Server // The underlying MCP server instance
	options   *options.Mcp
	extension *extension.Integration
}

func (s *Server) init() error {

	stream := false
	var newImplementer = extension.New(s.extension)
	var options = []server.Option{
		server.WithNewImplementer(newImplementer),
		server.WithImplementation(schema.Implementation{"Datly", "0.1"}),
		server.WithCapabilities(schema.ServerCapabilities{
			Resources: &schema.ServerCapabilitiesResources{ListChanged: &stream},
			Tools:     &schema.ServerCapabilitiesTools{ListChanged: &stream},
		}),
	}
	issuerURL := s.options.IssuerURL
	if s.options.Authorizer != "" {

		oauth2Config, err := loadAuthConfig(context.Background(), s.options)
		if err != nil {
			return err
		}
		if issuerURL == "" && oauth2Config != nil {
			issuerURL, _ = url.Base(oauth2Config.Endpoint.AuthURL, http.SecureScheme)
		}

		authCfg := &authorization.Config{
			ExcludeURI: "/sse",
			Global: &authorization.Authorization{
				ProtectedResourceMetadata: &meta.ProtectedResourceMetadata{
					Resource:             "https://datly.viantinc.com",
					AuthorizationServers: []string{issuerURL},
				},
				UseIdToken: true,
			},
		}
		options = append(options, server.WithAuthConfig(authCfg))
		switch s.options.Authorizer {
		case "F":

			memStore := store.NewMemoryStore(store.WithClientConfig(oauth2Config))
			roundTripper, err := transport.New(
				transport.WithStore(memStore),
				transport.WithAuthFlow(flow.NewBrowserFlow()),
			)
			if err != nil {
				return fmt.Errorf("failed to create auth round tripper: %w", err)
			}
			authServer, err := authserver.NewAuthServer(authCfg)
			if err != nil {
				return fmt.Errorf("failed to create auth server: %w", err)
			}
			fallbackAuth := authserver.NewFallbackAuth(authServer, roundTripper, roundTripper)
			options = append(options, server.WithAuthorizer(fallbackAuth.EnsureAuthorized))
		}
	}

	srv, err := server.New(options...)
	if err != nil {
		return err
	}
	s.server = srv
	return nil
}

func (s *Server) ListenAndServe() error {

	if s.options.Port == nil {
		stdio := s.server.Stdio(context.Background())
		err := stdio.ListenAndServe()
		if err != nil {
			fmt.Printf("Server error: %v\n", err)
			return err
		}
	} else {
		httpServer := s.server.HTTP(context.Background(), ":"+strconv.Itoa(*s.options.Port))
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

func loadAuthConfig(ctx context.Context, mcpOption *options.Mcp) (*oauth2.Config, error) {
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

func NewServer(extension *extension.Integration, mcp *options.Mcp) (*Server, error) {
	if extension == nil {
		return nil, nil
	}
	s := &Server{
		options:   mcp,
		extension: extension,
	}

	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}
