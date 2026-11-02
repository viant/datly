package tool_test

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
)

func TestTargetRequestPreservesAuthoredWithURIPathBinding(t *testing.T) {
	compiled, err := transcribe.NewCompiler().Compile(t.Context(), &transcribe.Source{Name: "Things", Scope: "test", Text: `#setting($_ = $route('/things','GET'))
#setting($_ = $mcp('Things'))
#define($_ = $Id<int>(path/id).WithURI('/{id}'))
SELECT id FROM things`})
	require.NoError(t, err)
	require.Len(t, compiled.Component.Routes, 2)
	alternative := compiled.Component.Routes[1]
	require.Equal(t, "/things/{id}", alternative.Path)
	typ := reflect.TypeOf(struct {
		Id     int `mcp:"name=Id"`
		Method string
		URI    string
	}{})
	bindings := []bindly.BindingSpec{
		{Path: "Id", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Method", Location: bindstate.Location{Kind: "http_request", In: "method"}},
		{Path: "URI", Location: bindstate.Location{Kind: "http_request", In: "uri"}},
	}
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	binding, err := injector.CompilePlan(typ, bindings...)
	require.NoError(t, err)
	projection, err := binding.Projection()
	require.NoError(t, err)
	route := spec.RouteRef{Method: alternative.Method, Path: alternative.Path}
	contract, err := registry.NewInputContract(typ, projection, registry.RouteInput{Route: route, Plan: binding, Bindings: bindings})
	require.NoError(t, err)
	input, ok := contract.ForRoute(route)
	require.True(t, ok)
	plan, err := tool.NewCompiler().Compile(tool.Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Things"}, Exposure: alternative.MCP[0], Contract: input})
	require.NoError(t, err)
	scope, err := plan.Scope(map[string]interface{}{"Id": 42, "Method": "DELETE", "URI": "/untrusted"})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Path().Locate(nil).Value(t.Context(), reflect.TypeFor[int](), "id")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "42", value)
	value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeFor[string](), "method")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, http.MethodGet, value)
	value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeFor[string](), "uri")
	require.NoError(t, err)
	require.True(t, found)
	// Request metadata describes the compiled route template. Bindly's separate
	// path provider retains actual values; this never dispatches a synthetic URL.
	require.Equal(t, "/things/%7Bid%7D", value)
	require.NotContains(t, plan.Metadata().InputSchema.Properties, "URI")
}
