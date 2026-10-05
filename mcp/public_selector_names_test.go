package mcp_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

type publicNamesInput struct {
	Fields  []string `parameter:"Fields,kind=query,in=fields" querySelector:"records"`
	OrderBy string   `parameter:"OrderBy,kind=query,in=sort" querySelector:"records"`
}
type publicNamesRow struct {
	CampaignId int    `sqlx:"campaign_id"`
	HTTPCode   int    `sqlx:"http_code"`
	Metric3    string `sqlx:"metric_3" json:"metric-3"`
	Secret     string `sqlx:"secret" internal:"true"`
}
type publicNamesOutput struct {
	Data []publicNamesRow `parameter:",kind=output,in=view"`
}

func TestCompiledPublicFieldNamesHTTPMCP(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(campaign_id INTEGER,http_code INTEGER,metric_3 TEXT,secret TEXT)", "INSERT INTO records VALUES(2,202,'second','private'),(1,201,'first','private')"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "PublicNames"}, Routes: []*spec.Route{{Method: "GET", Path: "/public", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "PublicNames"}}}}, Settings: &spec.Settings{CaseFormat: "lc"}, RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"campaign_id"}}, Source: &spec.ViewSource{SQL: "SELECT campaign_id,http_code,metric_3,secret FROM records"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[publicNamesInput](), OutputType: reflect.TypeFor[publicNamesOutput](), DirectViewField: "Data"})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(ctx, "PublicNames"))
	tool, ok := service.Registry().ToolRegistry.Get("PublicNames")
	require.True(t, ok)
	httpHandler := gateway.NewHandler(rt, nil, "test")
	for _, tc := range []struct {
		name     string
		fields   []string
		sort     string
		expected string
		bad      bool
	}{
		{"public camel", []string{"campaignId"}, "campaignId:asc", `{"data":[{"campaignId":1},{"campaignId":2}]}`, false},
		{"SQL name", []string{"campaign_id"}, "campaign_id DESC", `{"data":[{"campaignId":2},{"campaignId":1}]}`, false},
		{"custom numeric tag", []string{"metric-3"}, "campaignId ASC", `{"data":[{"metric-3":"first"},{"metric-3":"second"}]}`, false},
		{"CSV fields", []string{"campaignId", "metric-3"}, "campaignId ASC", `{"data":[{"campaignId":1,"metric-3":"first"},{"campaignId":2,"metric-3":"second"}]}`, false},
		{"hidden SQL", []string{"secret"}, "", "", true},
		{"hidden Go", []string{"Secret"}, "", "", true},
		{"unknown", []string{"missing"}, "", "", true},
		{"forbidden sort", []string{"campaignId"}, "metric-3 DESC", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := url.Values{"fields": tc.fields}
			if tc.name == "CSV fields" {
				q.Set("fields", strings.Join(tc.fields, ","))
			}
			if tc.sort != "" {
				q.Set("sort", tc.sort)
			}
			rec := httptest.NewRecorder()
			httpHandler.ServeHTTP(rec, httptest.NewRequest("GET", "/public?"+q.Encode(), nil))
			args := map[string]any{"Fields": tc.fields}
			if tc.sort != "" {
				args["OrderBy"] = tc.sort
			}
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "PublicNames", Arguments: args}})
			if tc.bad {
				require.Equal(t, 400, rec.Code, rec.Body.String())
				require.True(t, rpcErr != nil || result != nil && result.IsError != nil && *result.IsError)
				return
			}
			require.Equal(t, 200, rec.Code, rec.Body.String())
			require.JSONEq(t, tc.expected, rec.Body.String())
			require.Nil(t, rpcErr)
			body, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(body))
		})
	}
}
