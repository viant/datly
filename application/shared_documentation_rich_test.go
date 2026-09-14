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
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	xdocs "github.com/viant/xdatly/docs"
	"github.com/xeipuuv/gojsonschema"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type DocsIdentity struct {
	ID int `json:"identifier"`
}
type docsNode struct {
	DocsIdentity
	Label string    `json:"label" desc:"Authored label" example:"authored"`
	Next  *docsNode `json:"next,omitempty"`
}
type docsDependencyInput struct {
	Query   string    `parameter:"Query,kind=query,in=q,required=true" json:"query"`
	Payload *docsNode `parameter:"Payload,kind=body,in=payload" json:"payload"`
}
type docsDependency struct {
	Value string `json:"value"`
}
type docsParentInput struct {
	Dependency *docsDependency `parameter:"Dependency,kind=component,in=POST:/private-docs"`
}
type docsRelated struct {
	ID     int `sqlx:"id" json:"identifier"`
	UserID int `sqlx:"user_id" json:"user"`
}
type docsJoined struct {
	UserID   int           `sqlx:"user_id" json:"userIdentifier"`
	OrderID  int           `sqlx:"order_id" json:"orderIdentifier"`
	Related  []docsRelated `json:"related"`
	Archived []docsRelated `json:"archived"`
}
type docsJoinedOutput struct {
	Rows []docsJoined `parameter:"Rows,kind=output,in=view" json:"rows"`
}

func TestSharedDocumentationJoinedRelationsAndPrivateInputSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER)", "CREATE TABLE orders(id INTEGER,user_id INTEGER)", "INSERT INTO users VALUES(7)", "INSERT INTO orders VALUES(90,7)", "CREATE TABLE archived_orders(id INTEGER,user_id INTEGER)", "INSERT INTO archived_orders VALUES(91,7)"))
	var calls atomic.Int32
	manager, err := application.New(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Shutdown(ctx) })
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		store := resource.New()
		if err := store.Register("pkg", documented.Files); err != nil {
			return nil, err
		}
		private := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "private", Name: "Dependency"}, Documentation: xdocs.Source{DocURL: "pkg:rich-private.yaml"}, Routes: []*spec.Route{{Method: "POST", Path: "/private-docs"}}}
		child, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: private, InputType: reflect.TypeFor[docsDependencyInput](), OutputType: reflect.TypeFor[docsDependency](), Resources: store})
		if err != nil {
			return nil, err
		}
		childEntry, err := child.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(_ context.Context, inv handler.Invocation) (any, error) {
			calls.Add(1)
			input := inv.Input.(*docsDependencyInput)
			require.Equal(t, "needle", input.Query)
			require.NotNil(t, input.Payload)
			require.Equal(t, 7, input.Payload.ID)
			return &docsDependency{Value: "ready"}, nil
		})})
		if err != nil {
			return nil, err
		}
		parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "public", Name: "Joined"}, Documentation: xdocs.Source{DocURL: "pkg:rich-rule.yaml"}, Routes: []*spec.Route{{Method: "POST", Path: "/rich", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "rich"}}}}, RootView: &spec.View{Name: "joined", Source: &spec.ViewSource{SQL: "SELECT u.id AS user_id,o.id AS order_id FROM users u JOIN orders o ON o.user_id=u.id"}, Relations: []*spec.Relation{{Name: "related", Holder: "Related", Kind: spec.RelationKindSubview, Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "user_id", ChildColumn: "user_id"}}, View: &spec.View{Name: "orders", Source: &spec.ViewSource{Table: "orders", SQL: "SELECT id,user_id FROM orders WHERE $COLUMN_IN"}}}}}}
		parent.RootView.Relations = append(parent.RootView.Relations, &spec.Relation{Name: "archived", Holder: "Archived", Kind: spec.RelationKindSubview, Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "user_id", ChildColumn: "user_id"}}, View: &spec.View{Name: "archived_orders", Source: &spec.ViewSource{Table: "archived_orders", SQL: "SELECT id,user_id FROM archived_orders WHERE $COLUMN_IN"}}})
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parent, InputType: reflect.TypeFor[docsParentInput](), OutputType: reflect.TypeFor[docsJoinedOutput](), Resources: store})
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
		return &application.Build{Components: []*registry.RegisteredComponent{entry, childEntry}, Resources: store, Documentation: xdocs.Source{DocURL: "pkg:rich-global.yaml"}, HTTP: gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Rich", Version: "1"}}}}, nil
	}}))
	require.Equal(t, int32(0), calls.Load())
	raw, err := manager.ExportOpenAPI(ctx, openapi.ExportRequest{})
	require.NoError(t, err)
	var doc openapi3.OpenAPI
	require.NoError(t, json.Unmarshal(raw, &doc))
	op := doc.Paths["/rich"].Post
	require.Equal(t, "Joined user records", op.Description)
	require.Len(t, op.Parameters, 1)
	require.Equal(t, "Private query", op.Parameters[0].Description)
	require.True(t, op.Parameters[0].Required)
	var joined, related, input *openapi3.Schema
	for _, schema := range doc.Components.Schemas {
		if schema.Properties["userIdentifier"] != nil {
			joined = schema
		}
		if schema.Properties["identifier"] != nil && schema.Properties["user"] != nil {
			if schema.Properties["identifier"].Description == "Order identifier" {
				related = schema
			}
		}
		if schema.Properties["identifier"] != nil && schema.Properties["next"] != nil {
			input = schema
		}
	}
	require.NotNil(t, joined)
	require.Equal(t, "Rule user identifier", joined.Properties["userIdentifier"].Description)
	require.Equal(t, "Order identifier", joined.Properties["orderIdentifier"].Description)
	require.Equal(t, "Order records", joined.Properties["related"].Description)
	require.Equal(t, "Archived records", joined.Properties["archived"].Description)
	archivedRef := joined.Properties["archived"].Items.Ref
	require.Equal(t, "Archived identifier", doc.Components.Schemas[strings.TrimPrefix(archivedRef, "#/components/schemas/")].Properties["identifier"].Description)
	require.NotNil(t, related)
	require.Equal(t, "Order identifier", related.Properties["identifier"].Description)
	require.NotNil(t, input)
	require.Equal(t, "Private input identifier", input.Properties["identifier"].Description)
	require.Equal(t, "Authored label", input.Properties["label"].Description)
	require.NotContains(t, input.Properties, "DocsIdentity")
	_, service, err := manager.Pin(ctx)
	require.NoError(t, err)
	plan, ok := service.(*mcp.Service).Catalog().Tool("rich")
	require.True(t, ok)
	tool := plan.Metadata()
	require.Equal(t, "Private query", tool.InputSchema.Properties["query"]["description"])
	payload := tool.InputSchema.Properties["payload"]
	properties := payload["properties"].(map[string]any)
	require.Equal(t, "Private input identifier", properties["identifier"].(map[string]any)["description"])
	require.Equal(t, "Authored label", properties["label"].(map[string]any)["description"])
	require.Contains(t, payload, "$defs")
	encoded, err := json.Marshal(tool.InputSchema)
	require.NoError(t, err)
	_, err = gojsonschema.NewSchema(gojsonschema.NewBytesLoader(encoded))
	require.NoError(t, err)
	require.Equal(t, int32(0), calls.Load())
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/rich?q=needle", strings.NewReader(`{"payload":{"identifier":7,"label":"Input"}}`))
	request.Header.Set("Content-Type", "application/json")
	manager.ServeHTTP(recorder, request)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"rows":[{"userIdentifier":7,"orderIdentifier":90,"related":[{"identifier":90,"user":7}],"archived":[{"identifier":91,"user":7}]}]}`, recorder.Body.String())
	require.Equal(t, int32(1), calls.Load())
	if !testing.Short() {
		client := (mcpclient.Config{Source: manager, ProtocolVersion: schema.LatestProtocolVersion}).New(t)
		listed, err := client.ListTools(ctx, nil)
		require.NoError(t, err)
		require.Len(t, listed.Tools, 1)
		rendered, err := json.Marshal(listed.Tools[0])
		require.NoError(t, err)
		require.Contains(t, string(rendered), "Private input identifier")
		require.Contains(t, string(rendered), "$defs")
		result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "rich", Arguments: map[string]any{"query": "needle", "payload": map[string]any{"identifier": 7, "label": "Input"}}})
		require.NoError(t, err)
		body, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, recorder.Body.String(), string(body))
		require.Equal(t, int32(2), calls.Load())
	}

}
