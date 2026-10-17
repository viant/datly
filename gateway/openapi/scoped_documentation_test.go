package openapi_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/gateway/openapi"
	mcptool "github.com/viant/datly/mcp/tool"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
)

// A scoped dependency consumes the parent's transport input, but its private
// documentation must not replace the public authority's description or example.
func TestScopedDependencyDocumentationPublicSchemas(t *testing.T) {
	type childInput struct {
		Value string `parameter:"Value,kind=query,in=q"`
	}
	type parentInput struct {
		Value string            `parameter:"Value,kind=query,in=q,required=true"`
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/docs-child,scope=lazy"`
	}
	snapshot := func(text string) *docs.Snapshot {
		store := resource.New()
		require.NoError(t, store.Register("doc", fstest.MapFS{"params.yaml": &fstest.MapFile{Data: []byte("parameters:\n  Value: '" + text + " description'\n  Value$example: '" + text + " example'\n")}}))
		result, err := (docs.Loader{Resources: store}).Load(context.Background(), xdocs.Source{DocURL: "doc:params.yaml"})
		require.NoError(t, err)
		return result
	}
	parent := (fixture{component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DocsParent", Scope: "public"}, Routes: []*spec.Route{{Method: "GET", Path: "/docs-parent"}}}, input: reflect.TypeFor[parentInput](), output: reflect.TypeFor[dependencyResult]()}).registration(t)
	child := (fixture{component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DocsChild", Scope: "private"}, Routes: []*spec.Route{{Method: "GET", Path: "/docs-child"}}}, input: reflect.TypeFor[childInput](), output: reflect.TypeFor[dependencyResult]()}).registration(t)
	parent.Input = parent.Input.WithDocumentation(snapshot("public parent"))
	child.Input = child.Input.WithDocumentation(snapshot("private child"))
	entries := []*registry.RegisteredComponent{parent, child}
	catalog, err := registry.NewInputCatalog(entries)
	require.NoError(t, err)
	route := spec.RouteRef{Method: "GET", Path: "/docs-parent"}
	fields, err := catalog.Fields(route)
	require.NoError(t, err, "differing scoped child documentation must not invalidate transport coverage")
	require.Len(t, fields, 1)
	require.Equal(t, route, fields[0].Origin())
	contract, ok := parent.Input.ForRoute(route)
	require.True(t, ok)
	var childScope string
	for _, field := range contract.Fields() {
		if field.Path() == "Child" {
			childScope = field.Binding().Scope
		}
	}
	require.Equal(t, "lazy", childScope, "exercise scoped dependency coverage")
	plan, err := mcptool.NewCompiler().Compile(mcptool.Input{Fields: fields, Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "docs.parent"}})
	require.NoError(t, err)
	property := plan.Metadata().InputSchema.Properties["Value"]
	require.Equal(t, "public parent description", property["description"])
	mcpJSON, err := json.Marshal(plan.Metadata())
	require.NoError(t, err)
	require.Contains(t, string(mcpJSON), "public parent example")
	require.NotContains(t, string(mcpJSON), "private child")
	rt, err := druntime.NewRuntime(entries, druntime.WithExposedPackages([]string{"public"}, nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	request := documentRequest(entries...)
	request.Visibility = rt
	document, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, document.Paths, 1)
	parameters := document.Paths["/docs-parent"].Get.Parameters
	require.Len(t, parameters, 1)
	require.Equal(t, "public parent description", parameters[0].Description)
	require.Equal(t, "public parent example", parameters[0].Example)
	data, err := json.Marshal(document)
	require.NoError(t, err)
	require.NotContains(t, string(data), "private child")
}
