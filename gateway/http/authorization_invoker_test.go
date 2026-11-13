package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	custom "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestAuthorizationNativeComponentBinding(t *testing.T) {
	type input struct{ Token string }
	key := spec.Key{Kind: spec.KindComponent, Scope: "example/identity", Name: "Identity"}
	route := spec.RouteRef{Method: "GET", Path: "/internal/identity"}
	required := true
	component := &spec.Component{Key: key, Routes: []*spec.Route{{Method: route.Method, Path: route.Path, Internal: true}}, Parameters: []*spec.Parameter{{Name: "Token", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[authz.Facts]()})
	require.NoError(t, err)
	calls := 0
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[authz.Facts](), Handler: custom.NewFunc[input, authz.Facts](func(_ context.Context, in *input) (*authz.Facts, error) {
		calls++
		require.Equal(t, "verified-fixture", in.Token)
		return &authz.Facts{Subject: "user", Issuer: "fixture", Tenant: "tenant", ValidUntil: time.Now().Add(time.Minute)}, nil
	})}})
	require.NoError(t, err)
	defer rt.Shutdown(context.Background())
	c := authorizationFixture()
	c.Authorization.Exclude = nil
	c.AuthorizationProviders["identity"] = func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		invoker := AuthorizationComponentInvoker(ctx)
		require.NotNil(t, invoker)
		out, err := invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: key, Route: route}})
		if err != nil {
			return ctx, authz.Facts{}, err
		}
		return ctx, *out.(*authz.Facts), nil
	}
	h, err := c.Build(context.Background(), HandlerInput{Runtime: rt})
	require.NoError(t, err)
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("Authorization", "verified-fixture")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, 1, calls)
	missing := httptest.NewRecorder()
	h.ServeHTTP(missing, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, 401, missing.Code, missing.Body.String())
	require.Equal(t, 1, calls)
	require.Nil(t, AuthorizationComponentInvoker(context.Background()))
	// Internal dependency remains absent from public HTTP routes.
	require.Empty(t, rt.Routes())
}
