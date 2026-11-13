package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/scy/auth/jwt"
	xcontract "github.com/viant/xdatly/contract"
)

var discoveryFeature atomic.Bool

type discoveryCredentialOutput struct {
	Public  string `json:"public"`
	Feature string `json:"feature"`
}

func (*discoveryCredentialOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	visible := discoveryFeature.Load()
	return &xcontract.Policy{Name: fmt.Sprintf("feature:%s:%t", state.Claims.Subject, visible), Field: func(_ string, _ reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if field.Name == "Feature" && !visible {
			return field.Hide(), nil
		}
		return nil, nil
	}}, nil
}

// Discovery verifies credentials through the native JWT codec and refreshes
// current feature policy for the same token without executing business code.
func TestHTTPProtectedToolDiscoveryCredentialAndFeatureRefresh(t *testing.T) {
	key := testharness.NewJWT(t)
	codecs, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	require.NoError(t, err)
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DiscoveryCredential"}, Routes: []*spec.Route{{Method: "GET", Path: "/discovery-credential", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "discovery_credential"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[discoveryCredentialOutput](), CodecFactory: codecs})
	require.NoError(t, err)
	var calls atomic.Int32
	entry, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc(func(context.Context, *struct{}) (*discoveryCredentialOutput, error) {
		calls.Add(1)
		return &discoveryCredentialOutput{Public: "visible", Feature: "conditional"}, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	policy := &authorization.Policy{Tools: map[string]*authorization.Authorization{"discovery_credential": {ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "http://discovery.test/mcp"}}}}
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{entry}, Invoker: runtime, Authorization: policy})
	require.NoError(t, err)
	server := startWireServer(t, service)
	// Reuse one authenticated session and credential across feature changes.
	session := initializeWireSession(t, server.baseURL)
	token := key.Sign(t, "caller", time.Now().Add(time.Hour))
	payload := map[string]any{"jsonrpc": "2.0", "id": 2, "method": schema.MethodToolsList, "params": map[string]any{}}
	for _, visible := range []bool{false, true, false} {
		discoveryFeature.Store(visible)
		response, _, status := postWireRequest(t, server.baseURL+"/mcp", payload, session, token)
		require.Equal(t, http.StatusOK, status)
		require.Nil(t, response.Error)
		var result schema.ListToolsResult
		require.NoError(t, json.Unmarshal(response.Result, &result))
		require.Len(t, result.Tools, 1)
		_, present := result.Tools[0].OutputSchema.Properties["feature"]
		require.Equal(t, visible, present)
		require.Zero(t, result.TtlMs, "dynamic metadata must not become a retained client lease")
	}
	for _, invalid := range []string{"", "invalid.jwt", key.Sign(t, "expired", time.Now().Add(-time.Hour))} {
		response, _, status := postWireRequest(t, server.baseURL+"/mcp", payload, session, invalid)
		require.Equal(t, http.StatusOK, status)
		require.NotNil(t, response.Error, "a previous authenticated discovery must not supply this request's credential")
		require.Empty(t, response.Result)
	}
	require.Zero(t, calls.Load())
}

func TestPresentedBearerBridgePreservesCredentialAndSourceScope(t *testing.T) {
	for _, tc := range []struct{ presented, existing, want string }{
		{"Bearer fixture", "", "Bearer fixture"},
		{"Bearer fixture", "Bearer established", "Bearer established"},
		{"Basic ignored", "", ""},
		{"", "", ""},
	} {
		req := httptest.NewRequest("POST", "/mcp", nil)
		req.Header.Set("Authorization", tc.presented)
		if tc.existing != "" {
			req = req.WithContext(context.WithValue(req.Context(), authorization.TokenKey, &authorization.Token{Token: tc.existing}))
		}
		next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			actual := ""
			if token, ok := r.Context().Value(authorization.TokenKey).(*authorization.Token); ok && token != nil {
				actual = token.Token
			}
			require.Equal(t, tc.want, actual)
		})
		forwardPresentedBearer(next).ServeHTTP(httptest.NewRecorder(), req)
	}
	// A source without an authorizer or resource catalog retains its old scope.
	req := httptest.NewRequest("POST", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer unconfigured")
	forwardCatalogBearer(newTransportTestService(nil), http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		require.Nil(t, r.Context().Value(authorization.TokenKey))
	})).ServeHTTP(httptest.NewRecorder(), req)
}
