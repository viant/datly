package application_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/application/testdata/documented"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	xdocs "github.com/viant/xdatly/docs"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSharedDocumentationPublishedReloadSQLite(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER,name TEXT)", "INSERT INTO users VALUES(7,'Ada')"))
			manager, err := application.New(nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Shutdown(ctx) })
			compile := func(rule string) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
				return func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
					store := resource.New()
					if err := store.Register("pkg", documented.Files); err != nil {
						return nil, err
					}
					metadata, _, err := tag.ParseComponent(reflect.TypeFor[documented.Holder]().Field(0).Tag)
					if err != nil {
						return nil, err
					}
					if rule != "" {
						if err = store.Register("reload", fstest.MapFS{"rule.yaml": {Data: []byte(rule)}}); err != nil {
							return nil, err
						}
						metadata.Documentation.DocURL = "reload:rule.yaml"
					}
					component, err := (&bootstrap.RouteSource{PackagePath: "example.com/documented", Tag: metadata}).Resolve(reflect.TypeFor[documented.Input](), reflect.TypeFor[documented.Output]())
					if err != nil {
						return nil, err
					}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[documented.Input](), OutputType: reflect.TypeFor[documented.Output](), Resources: store})
					if err != nil {
						return nil, err
					}
					reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
					if err != nil {
						return nil, err
					}
					entry, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
					if err != nil {
						return nil, err
					}
					return &application.Build{Components: []*registry.RegisteredComponent{entry}, Resources: store, Documentation: xdocs.Source{DocURL: "pkg:global.yaml"}, HTTP: gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Docs", Version: "1"}}}}, nil
				}
			}
			require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: compile("")}))
			assertDocs := func(want string) {
				raw, err := manager.ExportOpenAPI(ctx, openapi.ExportRequest{})
				require.NoError(t, err)
				published := httptest.NewRecorder()
				manager.ServeHTTP(published, httptest.NewRequest("GET", gateway.DefaultOpenAPIURI, nil))
				require.Equal(t, 200, published.Code)
				require.JSONEq(t, string(raw), published.Body.String())
				var doc openapi3.OpenAPI
				require.NoError(t, json.Unmarshal(raw, &doc))
				op := doc.Paths["/records"].Get
				require.Equal(t, want+" records", op.Description)
				for _, p := range op.Parameters {
					switch p.Name {
					case "search":
						require.Equal(t, want+" search", p.Description)
						require.Equal(t, "needle", p.Example)
					case "explicit":
						require.Equal(t, "Authored parameter", p.Description)
						require.Equal(t, "authored-example", p.Example)
					}
				}
				matched := false
				for _, s := range doc.Components.Schemas {
					if p := s.Properties["identifier"]; p != nil {
						matched = true
						require.Equal(t, want+" identifier", p.Description)
						require.Equal(t, "9", p.Example)
						require.Equal(t, "integer", p.Type)
						require.Equal(t, "Authored name", s.Properties["name"].Description)
					}
				}
				require.True(t, matched)
			}
			assertDocs("Rule")
			var listTools func(context.Context, *schema.ListToolsRequestParams) (*schema.ListToolsResult, error)
			if !testing.Short() {
				client := (mcpclient.Config{Source: manager, ProtocolVersion: version}).New(t)
				listTools = func(ctx context.Context, _ *schema.ListToolsRequestParams) (*schema.ListToolsResult, error) {
					return client.ListTools(ctx, nil)
				}
			}
			assertMCP := func(want string) {
				if listTools == nil {
					listTools = func(ctx context.Context, _ *schema.ListToolsRequestParams) (*schema.ListToolsResult, error) {
						_, service, err := manager.Pin(ctx)
						if err != nil {
							return nil, err
						}
						factory, err := mcpserver.NewHandler(service)
						if err != nil {
							return nil, err
						}
						raw, err := factory(ctx, nil, nil, nil)
						if err != nil {
							return nil, err
						}
						protocol := raw.(*mcpserver.Handler)
						protocol.ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: version}
						result, protocolErr := protocol.ListTools(ctx, nil)
						if protocolErr != nil {
							return nil, protocolErr
						}
						encoded, err := json.Marshal(result.Tools)
						if err != nil {
							return nil, err
						}
						var wire []schema.Tool
						err = json.Unmarshal(encoded, &wire)
						return &schema.ListToolsResult{Tools: wire}, err
					}
				}
				result, err := listTools(ctx, nil)
				require.NoError(t, err)
				require.Len(t, result.Tools, 1)
				tool := result.Tools[0]
				require.Equal(t, want+" records", *tool.Description)
				require.Equal(t, want+" search", tool.InputSchema.Properties["query"]["description"])
				require.Equal(t, []interface{}{"needle"}, tool.InputSchema.Properties["query"]["examples"])
			}
			assertMCP("Rule")
			require.Error(t, manager.Reload(ctx, application.Request{Revision: 2, Compile: compile("Columns: [")}))
			require.Equal(t, uint64(1), manager.Revision())
			assertDocs("Rule")
			assertMCP("Rule")
			rule := strings.ReplaceAll("Columns:\n  users:\n    id: Reload identifier\n    id$example: '9'\nParameters:\n  Search: Reload search\nPaths:\n  /records: Reload records\n", "\\n", "\n")
			require.NoError(t, manager.Reload(ctx, application.Request{Revision: 3, Compile: compile(rule)}))
			assertDocs("Reload")
			assertMCP("Reload")
			recorder := httptest.NewRecorder()
			manager.ServeHTTP(recorder, httptest.NewRequest("GET", "/records", nil))
			require.Equal(t, 200, recorder.Code)
			require.JSONEq(t, `{"rows":[{"identifier":7,"name":"Ada"}]}`, recorder.Body.String())
		})
	}
}
