package tool

import (
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type targetRecord struct {
	Count   int     `json:"count"`
	Enabled bool    `json:"enabled"`
	Note    *string `json:"note"`
}
type targetRequestInput struct {
	Method  string
	URI     string
	Request *http.Request
	Trace   string         `mcp:"name=Trace"`
	Data    []targetRecord `mcp:"name=Data"`
}

func targetRequestPlan(t *testing.T, method string, typ reflect.Type, bindings []bindly.BindingSpec, strict bool) *Plan {
	t.Helper()
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	binding, err := injector.CompilePlan(typ, bindings...)
	require.NoError(t, err)
	projection, err := binding.Projection()
	require.NoError(t, err)
	route := spec.RouteRef{Method: method, Path: "/v1/fixture/items"}
	contract, err := registry.NewInputContract(typ, projection, registry.RouteInput{Route: route, Plan: binding, Bindings: bindings})
	require.NoError(t, err)
	input, ok := contract.ForRoute(route)
	require.True(t, ok)
	plan, err := NewCompiler(WithStrictArguments(strict)).Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/fixture", Name: "items"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "fixture.items"}, Contract: input})
	require.NoError(t, err)
	return plan
}
func TestOrdinaryMCPRetainsDeclaredTargetRequest(t *testing.T) {
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			plan := targetRequestPlan(t, method, reflect.TypeFor[targetRequestInput](), []bindly.BindingSpec{
				{Path: "Method", Location: bindstate.Location{Kind: "http_request", In: "method"}},
				{Path: "URI", Location: bindstate.Location{Kind: "http_request", In: "uri"}},
				{Path: "Request", Location: bindstate.Location{Kind: "http_request"}},
				{Path: "Trace", Location: bindstate.Location{Kind: "header", In: "X-Trace"}},
				{Path: "Data", Location: bindstate.Location{Kind: "body", In: "data"}},
			}, false)
			args := map[string]interface{}{"Trace": "fixture-trace", "Data": []interface{}{map[string]interface{}{"count": 0, "enabled": false, "note": nil}}, "Method": "POST"}
			before, err := json.Marshal(args)
			require.NoError(t, err)
			scope, err := plan.Scope(args)
			require.NoError(t, err)
			defer scope.Close()
			require.NotContains(t, plan.Metadata().InputSchema.Properties, "Method", "target method is not client authority")
			value, found, err := scope.Request().Locate(nil).Value(t.Context(), reflect.TypeOf(""), "method")
			require.NoError(t, err)
			require.True(t, found, "ordinary JSON MCP must expose canonical target request metadata")
			require.Equal(t, method, value)
			value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeOf(""), "uri")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "/v1/fixture/items", value)
			value, found, err = scope.Header().Locate(nil).Value(t.Context(), reflect.TypeOf(""), "X-Trace")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "fixture-trace", value)
			value, found, err = scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[[]targetRecord](), "data")
			require.NoError(t, err)
			require.True(t, found)
			encoded, err := json.Marshal(value)
			require.NoError(t, err)
			require.JSONEq(t, `[{"count":0,"enabled":false,"note":null}]`, string(encoded))
			value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeFor[*http.Request](), "")
			require.NoError(t, err)
			require.True(t, found)
			request := value.(*http.Request)
			require.Equal(t, method, request.Method)
			require.Equal(t, "/v1/fixture/items", request.URL.Path)
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			require.JSONEq(t, `{"data":[{"count":0,"enabled":false,"note":null}]}`, string(body))
			after, err := json.Marshal(args)
			require.NoError(t, err)
			require.Equal(t, before, after, "request projection must not mutate caller arguments")
		})
	}
}

func TestMCPMethodOverrideRetainsExistingStrictArgumentPolicy(t *testing.T) {
	plan := targetRequestPlan(t, http.MethodDelete, reflect.TypeFor[targetRequestInput](), []bindly.BindingSpec{{Path: "Method", Location: bindstate.Location{Kind: "http_request", In: "method"}}, {Path: "Data", Location: bindstate.Location{Kind: "body", In: "data"}}}, true)
	_, err := plan.Scope(map[string]interface{}{"Data": []interface{}{}, "Method": "PATCH"})
	require.Error(t, err, "existing strict mode rejects undeclared Method; permissive mode cannot override target either")
}

func TestBinaryMCPRetainsDeclaredTargetRequest(t *testing.T) {
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			typ := reflect.TypeOf(struct {
				Files  []*multipart.FileHeader `mcp:"name=Files"`
				Action string                  `mcp:"name=Action"`
			}{})
			plan := targetRequestPlan(t, method, typ, []bindly.BindingSpec{{Path: "Files", Location: bindstate.Location{Kind: "form", In: "file"}}, {Path: "Action", Location: bindstate.Location{Kind: "form", In: "uploadAction"}}}, false)
			scope, err := plan.Scope(map[string]interface{}{"Files": originalBlob("binary fixture"), "Action": "fixture"})
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Request().Locate(nil).Value(t.Context(), reflect.TypeOf(""), "method")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, method, value)
			value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeOf(""), "uri")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "/v1/fixture/items", value)
			value, found, err = scope.Form().Locate(nil).Value(t.Context(), reflect.TypeFor[[]*multipart.FileHeader](), "file")
			require.NoError(t, err)
			require.True(t, found)
			file, err := value.([]*multipart.FileHeader)[0].Open()
			require.NoError(t, err)
			defer file.Close()
			body, err := io.ReadAll(file)
			require.NoError(t, err)
			require.Equal(t, "binary fixture", string(body))
		})
	}
}
