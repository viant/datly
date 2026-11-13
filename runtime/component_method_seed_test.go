package runtime

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	xhandler "github.com/viant/xdatly/handler"
)

type methodSeedInput struct {
	Method *string                `parameter:"Method,kind=http_request,in=method"`
	Has    *struct{ Method bool } `setMarker:"true" json:"-"`
}
type methodSeedOutput struct{ Method, Transport string }
type methodSeedChild struct{}

func (methodSeedChild) Exec(ctx context.Context, session xhandler.Session, input *methodSeedInput, output *methodSeedOutput) error {
	if input.Method != nil {
		output.Method = *input.Method
	}
	var transport struct {
		Method string `bind:"kind=http_request,in=method"`
	}
	if err := session.Binder().Bind(ctx, &transport); err != nil {
		return err
	}
	output.Transport = transport.Method
	return nil
}

type methodSeedParent struct {
	target xhandler.ComponentTarget
	seed   *methodSeedInput
}

func (p methodSeedParent) Exec(ctx context.Context, session xhandler.Session, _ *struct{}, output *methodSeedOutput) error {
	value, found, err := session.Binder().Lookup(ctx, xhandler.ComponentInvokerKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("native component invoker unavailable")
	}
	value, err = value.(xhandler.ComponentInvoker).Invoke(ctx, xhandler.ComponentRequest{Target: p.target, Input: p.seed})
	if err != nil {
		return err
	}
	*output = *value.(*methodSeedOutput)
	return nil
}

func methodSeedRuntime(t *testing.T, parentMethod, childMethod string, seed *methodSeedInput) *Runtime {
	t.Helper()
	child := componentArtifact(t, componentSpec("MethodSeedChild", childMethod, "/method-child", nil), reflect.TypeFor[methodSeedInput](), reflect.TypeFor[methodSeedOutput]())
	child.Handler = custom.New[methodSeedInput, methodSeedOutput](methodSeedChild{})
	parent := componentArtifact(t, componentSpec("MethodSeedParent", parentMethod, "/method-parent", nil), reflect.TypeFor[struct{}](), reflect.TypeFor[methodSeedOutput]())
	parent.Handler = custom.New[struct{}, methodSeedOutput](methodSeedParent{target: xhandler.ComponentTarget{Scope: child.Component.Key.Scope, Name: child.Component.Key.Name, Method: childMethod, Path: "/method-child"}, seed: seed})
	var entries []*registry.RegisteredComponent
	for _, artifact := range []*bootstrap.Artifact{parent, child} {
		entry, err := artifact.Registration(registry.RegisteredComponent{})
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	runtime, err := NewRuntime(entries)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	return runtime
}

func TestNativeComposedTrustedMethodSeed(t *testing.T) {
	for _, parentMethod := range []string{"POST", "PATCH"} {
		for _, childMethod := range []string{"POST", "PATCH"} {
			t.Run(parentMethod+" to "+childMethod, func(t *testing.T) {
				seed := &methodSeedInput{Method: &childMethod, Has: &struct{ Method bool }{true}}
				runtime := methodSeedRuntime(t, parentMethod, childMethod, seed)
				httpRequest := httptest.NewRequest(parentMethod, "/method-parent", nil)
				scope, err := request.New(httpRequest)
				require.NoError(t, err)
				defer scope.Close()
				value, err := runtime.ExecuteRoute(t.Context(), parentMethod, "/method-parent", scope)
				require.NoError(t, err)
				output := value.(*methodSeedOutput)
				require.Equal(t, childMethod, output.Method, "trusted already-bound scalar must retain explicit suppliedness")
				require.Equal(t, parentMethod, output.Transport, "the shared original transport provider must remain unchanged")
				require.Equal(t, parentMethod, httpRequest.Method)
			})
		}
	}
}

func TestNativeComposedOmittedMethodRetainsTransportBinding(t *testing.T) {
	for _, parentMethod := range []string{"POST", "PATCH"} {
		runtime := methodSeedRuntime(t, parentMethod, "GET", &methodSeedInput{Has: &struct{ Method bool }{false}})
		scope, err := request.New(httptest.NewRequest(parentMethod, "/method-parent", nil))
		require.NoError(t, err)
		value, err := runtime.ExecuteRoute(t.Context(), parentMethod, "/method-parent", scope)
		require.NoError(t, err)
		require.NoError(t, scope.Close())
		require.Equal(t, parentMethod, value.(*methodSeedOutput).Method)
	}
}

func TestNativeHTTPBodyMethodCannotOverrideTransport(t *testing.T) {
	runtime := methodSeedRuntime(t, "POST", "PATCH", &methodSeedInput{})
	httpRequest := httptest.NewRequest("PATCH", "/method-child", strings.NewReader(`{"method":"DELETE","Has":{"Method":true}}`))
	httpRequest.Header.Set("Content-Type", "application/json")
	scope, err := request.New(httpRequest)
	require.NoError(t, err)
	defer scope.Close()
	value, err := runtime.ExecuteRoute(t.Context(), "PATCH", "/method-child", scope)
	require.NoError(t, err)
	require.Equal(t, "PATCH", value.(*methodSeedOutput).Method)
}
