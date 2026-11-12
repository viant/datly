package runtime_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/contract"
)

type reuseAuthInput struct {
	Role string `parameter:"Role,kind=header,in=X-Role,required=true"`
}
type reuseAuthOutput struct{ Role string }
type reuseBusinessInput struct {
	Auth *reuseAuthOutput `parameter:"Auth,kind=component,in=GET:/reuse-auth,required=true"`
}
type reuseBusinessOutput struct {
	Role string `json:"role"`
}

func (*reuseBusinessOutput) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	var state struct {
		Auth *reuseAuthOutput `parameter:"Auth,kind=component,in=GET:/reuse-auth,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return nil, nil
}

// One transport operation must reuse its declared dependency across metadata
// and execution, while the next operation must obtain fresh dependency values.
func TestHTTPContractAndExecutionReuseDeclaredDependency(t *testing.T) {
	var authCalls, businessCalls atomic.Int64
	component := func(name, path string) *spec.Component {
		return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reuse", Name: name}, Routes: []*spec.Route{{Method: "GET", Path: path}}}
	}
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component("auth", "/reuse-auth"), InputType: reflect.TypeFor[reuseAuthInput](), OutputType: reflect.TypeFor[reuseAuthOutput](), Handler: custom.NewFunc(func(_ context.Context, in *reuseAuthInput) (*reuseAuthOutput, error) {
		authCalls.Add(1)
		return &reuseAuthOutput{Role: in.Role}, nil
	})})
	require.NoError(t, err)
	business, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component("business", "/reuse-business"), InputType: reflect.TypeFor[reuseBusinessInput](), OutputType: reflect.TypeFor[reuseBusinessOutput](), Handler: custom.NewFunc(func(_ context.Context, in *reuseBusinessInput) (*reuseBusinessOutput, error) {
		businessCalls.Add(1)
		return &reuseBusinessOutput{Role: in.Auth.Role}, nil
	})})
	require.NoError(t, err)
	authEntry, err := auth.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	businessEntry, err := business.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{authEntry, businessEntry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	gateway := httpgateway.NewHandler(runtime, nil, "reuse-test")
	for _, role := range []string{"first", "second"} {
		t.Run(role, func(t *testing.T) {
			before := authCalls.Load()
			req := httptest.NewRequest("GET", "/reuse-business", nil)
			req.Header.Set("X-Role", role)
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, req)
			require.Equal(t, 200, response.Code, response.Body.String())
			require.JSONEq(t, `{"role":"`+role+`"}`, response.Body.String())
			require.EqualValues(t, 1, authCalls.Load()-before, "same request resolved its dependency more than once")
		})
	}
	require.EqualValues(t, 2, businessCalls.Load())
}
