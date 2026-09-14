package application_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
)

type docsInput struct {
	ID int `parameter:"ID,kind=path,in=id,required=true"`
}
type docsOutput struct {
	Value string `json:"value"`
}
type docsFixture struct{ config gateway.Config }

func (f *docsFixture) compile(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
	var entries []*registry.RegisteredComponent
	for _, definition := range []struct {
		name, scope, path string
		input             reflect.Type
		methods           []string
	}{
		{"Items", "public", "/api/items/{id}", reflect.TypeFor[docsInput](), []string{"GET"}},
		{"Actions", "public", "/api/actions", reflect.TypeFor[struct{}](), []string{"GET", "POST"}},
		{"Outside", "public", "/outside", reflect.TypeFor[struct{}](), []string{"GET"}},
		{"Private", "private", "/api/items/private", reflect.TypeFor[struct{}](), []string{"GET"}},
	} {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: definition.name, Scope: definition.scope}}
		for _, method := range definition.methods {
			component.Routes = append(component.Routes, &spec.Route{Method: method, Path: definition.path, APIKeyHeader: "X-Service", APIKeyValue: "service-secret"})
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: definition.input, OutputType: reflect.TypeFor[docsOutput](), Types: types})
		if err != nil {
			return nil, err
		}
		entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) { return &docsOutput{Value: "ok"}, nil })})
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &application.Build{HTTP: f.config, Components: entries, RuntimeOptions: []druntime.Option{druntime.WithExposedPackages([]string{"public"}, nil)}}, nil
}

func TestOpenAPIApplicationAccessExactRoutesAndAtomicPolicy(t *testing.T) {
	ctx := context.Background()
	origins := []string{"https://docs.example"}
	methods := []string{"GET", "HEAD"}
	headers := []string{"X-Docs"}
	f := &docsFixture{config: gateway.Config{APIPrefix: "/api", Meta: gateway.Meta{CacheWarmURI: " ", DocURI: "/docs"}, CORS: &spec.CORS{AllowOrigins: &origins, AllowMethods: &methods, AllowHeaders: &headers}, OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Staged API", Version: "one"}}}}
	manager, err := application.New(nil)
	require.NoError(t, err)
	defer manager.Shutdown(ctx)
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile}))
	call := func(ctx context.Context, path, method, key string, status int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil).WithContext(ctx)
		req.Header.Set("Origin", origins[0])
		if key != "" {
			req.Header.Set("X-Docs", key)
		}
		response := httptest.NewRecorder()
		manager.ServeHTTP(response, req)
		require.Equal(t, status, response.Code, response.Body.String())
		return response
	}
	// Original OpenAPI routes are appended after the derived-route API-key loop.
	// No business API key is necessary to fetch the original public document surface.
	response := call(ctx, gateway.DefaultOpenAPIURI, "GET", "", 200)
	require.Equal(t, origins[0], response.Header().Get("Access-Control-Allow-Origin"))
	require.NotContains(t, response.Body.String(), "service-secret")
	var doc openapi3.OpenAPI
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &doc))
	require.Len(t, doc.Paths, 2)
	require.NotContains(t, doc.Paths, "/outside")
	require.NotContains(t, doc.Paths, "/api/items/private")
	require.NotNil(t, doc.Paths["/api/actions"].Get)
	require.NotNil(t, doc.Paths["/api/actions"].Post)
	require.NotEmpty(t, doc.Components.SecuritySchemes, "business security is still described")
	call(ctx, "/api/actions", "GET", "", 403)
	call(ctx, gateway.DefaultOpenAPIURI+"/items/7", "GET", "", 200)
	call(ctx, gateway.DefaultOpenAPIURI+"/items/private", "GET", "", 404)
	call(ctx, gateway.DefaultOpenAPIURI+"/missing", "GET", "", 404)
	call(ctx, gateway.DefaultOpenAPIURI+"/outside", "GET", "", 404)
	call(ctx, gateway.DefaultOpenAPIURI+"/actions", "POST", "", 405)
	bytes, err := manager.ExportOpenAPI(ctx, openapi.ExportRequest{Path: "/api/actions"})
	require.NoError(t, err)
	require.Contains(t, string(bytes), "openapi: 3.0.1")
	_, err = manager.ExportOpenAPI(ctx, openapi.ExportRequest{Path: "/api/items/7"})
	require.Error(t, err, "trusted export selects exact authored path groups")
	pinned, _, err := manager.Pin(ctx)
	require.NoError(t, err)
	f.config.OpenAPI.AggregateAccess = &gateway.DocumentAccess{APIKeyHeader: "X-Docs", APIKeyValue: "aggregate"}
	f.config.OpenAPI.RouteAccess = &gateway.DocumentAccess{APIKeyHeader: "X-Docs", APIKeyValue: "routes"}
	f.config.OpenAPI.Info.Version = "two"
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 2, Compile: f.compile}))
	// Configuration mutation cannot change a published snapshot or access rule.
	f.config.OpenAPI.AggregateAccess.APIKeyValue = "later"
	call(ctx, gateway.DefaultOpenAPIURI, "GET", "", 403)
	call(ctx, gateway.DefaultOpenAPIURI, "GET", "aggregate", 200)
	call(ctx, gateway.DefaultOpenAPIURI+"/actions", "GET", "aggregate", 403)
	call(ctx, gateway.DefaultOpenAPIURI+"/actions", "GET", "routes", 200)
	call(ctx, "/docs", "GET", "", 403)
	call(ctx, "/docs", "GET", "aggregate", 200)
	require.Equal(t, response.Body.String(), call(pinned, gateway.DefaultOpenAPIURI, "GET", "", 200).Body.String())
	req := httptest.NewRequest("OPTIONS", gateway.DefaultOpenAPIURI, nil)
	req.Header.Set("Origin", origins[0])
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "X-Docs")
	preflight := httptest.NewRecorder()
	manager.ServeHTTP(preflight, req)
	require.Equal(t, 204, preflight.Code)
	for _, bad := range []struct {
		name  string
		apply func(*gateway.Config)
	}{
		{"invalid access", func(c *gateway.Config) {
			c.OpenAPI.RouteAccess = &gateway.DocumentAccess{APIKeyHeader: "Bad Header", APIKeyValue: "x"}
		}},
		{"component collision", func(c *gateway.Config) { c.Meta.OpenApiURI = "/api/actions" }},
		{"warmup collision", func(c *gateway.Config) { c.Meta.CacheWarmURI = gateway.DefaultOpenAPIURI }},
		{"UI collision", func(c *gateway.Config) { c.Meta.DocURI = gateway.DefaultOpenAPIURI }},
	} {
		t.Run(bad.name, func(t *testing.T) {
			configuration := f.config
			policy := *configuration.OpenAPI
			configuration.OpenAPI = &policy
			bad.apply(&configuration)
			invalid := docsFixture{config: configuration}
			err := manager.Reload(ctx, application.Request{Revision: 3, Compile: invalid.compile})
			require.Error(t, err)
			require.EqualValues(t, 2, manager.Revision())
			call(ctx, gateway.DefaultOpenAPIURI, "GET", "aggregate", 200)
		})
	}
}

func TestOpenAPIStageUsesConfiguredInputJWTWithoutPrincipal(t *testing.T) {
	ctx := context.Background()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	factory, err := auth.New(ctx, &auth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "openapi-application", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}})
	require.NoError(t, err)
	type input struct {
		JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
	}
	calls := 0
	manager, err := application.New(nil)
	require.NoError(t, err)
	defer manager.Shutdown(ctx)
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Primary"}, Routes: []*spec.Route{{Method: "GET", Path: "/primary"}}}, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[docsOutput](), CodecFactory: factory})
		if err != nil {
			return nil, err
		}
		entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) {
			calls++
			return nil, fmt.Errorf("unexpected document execution")
		})})
		if err != nil {
			return nil, err
		}
		return &application.Build{Components: []*registry.RegisteredComponent{entry}, HTTP: gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Primary", Version: "1"}}}}, nil
	}}))
	response := httptest.NewRecorder()
	manager.ServeHTTP(response, httptest.NewRequest("GET", gateway.DefaultOpenAPIURI, nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	var doc openapi3.OpenAPI
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &doc))
	require.Contains(t, doc.Components.SecuritySchemes, "BearerJWT")
	require.NotNil(t, doc.Paths["/primary"].Get.Security)
	require.Equal(t, 0, calls, "documentation must not invoke handlers, JWT binding, or an ambient principal")
	denied := httptest.NewRecorder()
	manager.ServeHTTP(denied, httptest.NewRequest("GET", "/primary", nil))
	require.Equal(t, 401, denied.Code)
	require.Equal(t, 0, calls)
}

func TestOpenAPIMetadataRemoteAddressAdmission(t *testing.T) {
	ctx := context.Background()
	prefixes := []string{"192.0.2."}
	f := &docsFixture{config: gateway.Config{APIPrefix: "/api", Meta: gateway.Meta{CacheWarmURI: " ", AllowedSubnet: prefixes}, OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Restricted", Version: "one"}}}}
	manager, err := application.New(nil)
	require.NoError(t, err)
	defer manager.Shutdown(ctx)
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile}))
	prefixes[0] = "127." // published policy was detached
	for _, path := range []string{gateway.DefaultOpenAPIURI, gateway.DefaultOpenAPIURI + "/items/7", "/api/actions"} {
		for _, peer := range []string{"192.0.2.9:1000", "127.0.0.1:1000"} {
			req := httptest.NewRequest("GET", path, nil)
			req.RemoteAddr = peer
			req.Header.Set("X-Service", "service-secret")
			req.Header.Set("X-Forwarded-For", "192.0.2.9")
			response := httptest.NewRecorder()
			manager.ServeHTTP(response, req)
			expected := 200
			if peer == "127.0.0.1:1000" {
				expected = 403
			}
			require.Equal(t, expected, response.Code, response.Body.String())
		}
	}
}
