package application_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	xdocs "github.com/viant/xdatly/docs"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
)

func TestSharedDocumentationResponseResourceReload(t *testing.T) {
	ctx := context.Background()
	manager, err := application.New(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Shutdown(ctx) })
	var calls atomic.Int32
	dictionary := `Paths:
  /schema-response: Schema-described response
Responses:
  /schema-response:
    '201':
      description: Authored response
      headers:
        X-Authored:
          schema: {type: string}
          example: yes
      content:
        application/json:
          schema: {$ref: 'schemas/body.json'}
          example: {message: illustrative}
`
	compile := func(doc, body string) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
			store := resource.New()
			files := fstest.MapFS{"docs.yaml": {Data: []byte(doc)}}
			if body != "" {
				files["schemas/body.json"] = &fstest.MapFile{Data: []byte(body)}
			}
			if err := store.Register("p", files); err != nil {
				return nil, err
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "SchemaResponse"}, Documentation: xdocs.Source{DocURL: "p:docs.yaml"}, Routes: []*spec.Route{{Method: "GET", Path: "/schema-response", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "schema-response"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[response.Response](), Resources: store})
			if err != nil {
				return nil, err
			}
			entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) {
				calls.Add(1)
				return response.NewBuffered(response.WithBytes([]byte{255, 0, 2}), response.WithStatusCode(201)), nil
			})})
			if err != nil {
				return nil, err
			}
			return &application.Build{Components: []*registry.RegisteredComponent{entry}, Resources: store, HTTP: gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Schemas", Version: "1"}}}}, nil
		}
	}
	first := `{"type":"object","properties":{"message":{"type":"string","description":"Version one"},"next":{"$ref":"#"}}}`
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: compile(dictionary, first)}))
	require.Equal(t, int32(0), calls.Load())
	var wire func() []schema.Tool
	if !testing.Short() {
		client := (mcpclient.Config{Source: manager, ProtocolVersion: schema.LatestProtocolVersion}).New(t)
		wire = func() []schema.Tool {
			listed, err := client.ListTools(ctx, nil)
			require.NoError(t, err)
			return listed.Tools
		}
	}
	assert := func(want, absent string) {
		raw, err := manager.ExportOpenAPI(ctx, openapi.ExportRequest{})
		require.NoError(t, err)
		require.Contains(t, string(raw), want)
		require.NotContains(t, string(raw), absent)
		_, current, err := manager.Pin(ctx)
		require.NoError(t, err)
		plan, ok := current.(*mcp.Service).Catalog().Tool("schema-response")
		require.True(t, ok)
		tool := plan.Metadata()
		if wire != nil {
			listed := wire()
			require.Len(t, listed, 1)
			tool = listed[0]
		}
		require.Nil(t, tool.OutputSchema)
		data, err := json.Marshal(tool.Meta)
		require.NoError(t, err)
		require.Contains(t, string(data), want)
		require.NotContains(t, string(data), absent)
		require.Contains(t, tool.Meta, "datly/httpSchemas")
	}
	assert("Version one", "Version two")
	for _, test := range []struct{ name, doc, body string }{
		{"missing document", "Paths: [", first}, {"missing schema", dictionary, ""},
		{"malformed schema", dictionary, "{"}, {"unknown type", dictionary, `{"type":"not-a-type"}`},
		{"bad constraint", dictionary, `{"type":"string","minLength":-2}`},
		{"draft7 contains", dictionary, `{"type":"array","contains":{"type":"integer"}}`},
		{"draft7 conditional", dictionary, `{"if":{"type":"string"},"then":{"minLength":3}}`},
		{"newer keyword in unused definition", dictionary, `{"type":"object","definitions":{"unused":{"dependentSchemas":{"a":{"required":["b"]}}}}}`},
		{"dangling reference", dictionary, `{"$ref":"#/definitions/missing"}`},
		{"no network", dictionary, `{"$ref":"https://example.invalid/never-fetch.json"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, manager.Reload(ctx, application.Request{Revision: 2, Compile: compile(test.doc, test.body)}))
			require.Equal(t, uint64(1), manager.Revision())
			assert("Version one", "Version two")
			require.Equal(t, int32(0), calls.Load())
		})
	}
	second := strings.ReplaceAll(first, "Version one", "Version two")
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 3, Compile: compile(dictionary, second)}))
	assert("Version two", "Version one")
	require.Equal(t, int32(0), calls.Load())
	recorder := httptest.NewRecorder()
	manager.ServeHTTP(recorder, httptest.NewRequest("GET", "/schema-response", nil))
	require.Equal(t, 201, recorder.Code)
	require.Equal(t, []byte{255, 0, 2}, recorder.Body.Bytes())
	require.Equal(t, int32(1), calls.Load())
}
