package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

type sourceSelectorRow struct {
	TaxonomyID   int    `sqlx:"ID|TAXONOMY_ID" source:"ID" json:"taxonomyId"`
	TaxonomyName string `sqlx:"NAME|TAXONOMY_NAME" source:"NAME" json:"taxonomyName"`
	Secret       string `sqlx:"SECRET" source:"private" json:"-"`
}
type sourceSelectorInput struct {
	Fields []string `parameter:"Fields,kind=query,in=fields"`
	Page   int      `parameter:"Page,kind=query,in=page"`
}
type sourceSelectorOutput struct {
	Data []sourceSelectorRow `parameter:",kind=output,in=view" json:"data"`
}

func TestDeclaredSourceSelectorHTTPMCP(t *testing.T) {
	for _, mode := range []string{"eager", "indexed", "eager-disabled", "indexed-disabled", "eager-denied", "indexed-denied"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE seeds(ID INTEGER,NAME TEXT,SECRET TEXT,tenant_id INTEGER)", "INSERT INTO seeds VALUES(1,'one','hidden',7),(2,'two','hidden',7),(3,'three','hidden',7),(4,'outside','hidden',8)"))
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "SeedSource"}, Routes: []*spec.Route{{Method: "GET", Path: "/seed-source", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "SeedSource"}}}}, RootView: &spec.View{Name: "seeds", Selector: &spec.Selector{AllowFields: true, AllowPage: true, NoLimit: true}, Source: &spec.ViewSource{SQL: "SELECT ID AS TAXONOMY_ID,NAME AS TAXONOMY_NAME,SECRET FROM seeds WHERE tenant_id=7 ORDER BY ID"}}, Parameters: []*spec.Parameter{
				{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "seeds", Property: spec.SelectorPropertyFields}},
				{Name: "Page", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "seeds", Property: spec.SelectorPropertyPage}},
			}}
			disabled := strings.HasSuffix(mode, "-disabled")
			component.RootView.Selector.AllowFields = !disabled
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[sourceSelectorInput](), OutputType: reflect.TypeFor[sourceSelectorOutput](), DirectViewField: "Data"})
			require.NoError(t, err)
			// Explicit selector binding infers enabled authoring settings. Exercise
			// the execution permission guard with an explicitly disabled compiled plan.
			artifact.Reader.Root.View.Spec.Selector.AllowFields = !disabled
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			var rt *druntime.Runtime
			config := mcp.Config{Components: []*druntime.RegisteredComponent{registered}}
			if strings.HasPrefix(mode, "indexed") {
				rt, err = druntime.NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, caseLoader{registered})
				config.Components = nil
				config.Indexed = []*spec.Component{artifact.Component}
				config.Loader = rt
			} else {
				rt, err = druntime.NewRuntime(config.Components)
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			denied := strings.HasSuffix(mode, "-denied")
			if denied {
				config.AuthorizeTool = func(context.Context, dexec.ComponentTarget) error { return errors.New("access denied") }
			}
			config.Invoker = rt
			service, err := mcp.New(config)
			require.NoError(t, err)
			require.NoError(t, service.PrepareTool(ctx, "SeedSource"))
			tool, ok := service.Registry().ToolRegistry.Get("SeedSource")
			require.True(t, ok)
			handler := gateway.NewHandler(rt, nil, "test")
			if denied {
				handler, err = (gateway.Config{Authorize: func(context.Context, *http.Request, dexec.ComponentTarget) error { return errors.New("access denied") }}).NewHandler(rt, nil, "test")
				require.NoError(t, err)
			}
			for _, tc := range []struct {
				name   string
				fields []string
				page   string
				want   string
			}{
				{"source", []string{"id"}, "", `{"data":[{},{},{}]}`},
				{"source case", []string{"ID"}, "", `{"data":[{},{},{}]}`},
				{"source page", []string{"id"}, "1", `{"data":[{},{},{}]}`},
				{"canonical", []string{"taxonomyId"}, "", `{"data":[{"taxonomyId":1},{"taxonomyId":2},{"taxonomyId":3}]}`},
				{"mixed", []string{"id", "taxonomyName"}, "", `{"data":[{"taxonomyName":"one"},{"taxonomyName":"two"},{"taxonomyName":"three"}]}`},
				{"same result", []string{"id", "taxonomyId"}, "", `{"data":[{"taxonomyId":1},{"taxonomyId":2},{"taxonomyId":3}]}`},
				{"unknown", []string{"missing"}, "", ""}, {"qualified", []string{"t.ID"}, "", ""}, {"hidden source", []string{"private"}, "", ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := url.Values{"fields": tc.fields}
					args := map[string]any{"Fields": tc.fields}
					if tc.page != "" {
						query.Set("page", tc.page)
						args["Page"] = 1
					}
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, httptest.NewRequest("GET", "/seed-source?"+query.Encode(), nil))
					result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SeedSource", Arguments: args}})
					if tc.want == "" || disabled || denied {
						require.NotEqual(t, 200, rec.Code)
						require.True(t, rpcErr != nil || result != nil && result.IsError != nil && *result.IsError)
						return
					}
					require.Equal(t, 200, rec.Code, rec.Body.String())
					require.JSONEq(t, tc.want, rec.Body.String())
					require.Nil(t, rpcErr)
					require.NotNil(t, result)
					if result.IsError != nil {
						require.False(t, *result.IsError, "%+v", result)
					}
					body, err := json.Marshal(result.StructuredContent)
					require.NoError(t, err)
					require.JSONEq(t, tc.want, string(body))
					require.JSONEq(t, tc.want, result.Content[0].(schema.TextContent).Text)
				})
			}
		})
	}
}
