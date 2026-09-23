package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
	"github.com/xeipuuv/gojsonschema"
)

type schemaSummary struct {
	PageCount   *int `sqlx:"PAGE_COUNT"`
	RecordCount int  `sqlx:"RECORD_COUNT"`
}

type schemaCaseRow struct {
	AdvertiserId int
	Explicit     string `json:"Exact_Name" format:"name=Ignored"`
	Formatted    string `format:"name=DisplayName"`
	Hidden       string `json:"-"`
	Excluded     string
}

type schemaCaseOutput struct {
	response.Status
	Meta *schemaSummary `json:"meta"`
	Rows []schemaCaseRow
}

func TestMCPOutputSchemaMatchesCaseFormattedResponse(t *testing.T) {
	for _, mode := range []string{"eager", "indexed", "implicit output plan"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			component := &spec.Component{
				Key:      spec.Key{Kind: spec.KindComponent, Name: "Summary"},
				Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Rows.Excluded"}}},
				Routes:   []*spec.Route{{Method: "GET", Path: "/summary", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Summary"}}}},
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
				Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[schemaCaseOutput](),
			})
			require.NoError(t, err)
			value := &schemaCaseOutput{
				Status: response.Status{Status: "ok"}, Meta: &schemaSummary{RecordCount: 1},
				Rows: []schemaCaseRow{{AdvertiserId: 7, Explicit: "fixed", Formatted: "label", Hidden: "private", Excluded: "private"}},
			}
			registered, err := artifact.Registration(druntime.RegisteredComponent{
				Handler: custom.NewFunc[struct{}, schemaCaseOutput](func(context.Context, *struct{}) (*schemaCaseOutput, error) { return value, nil }),
			})
			require.NoError(t, err)
			if mode == "implicit output plan" {
				registered.Output = nil
			}
			config := mcp.Config{Components: []*druntime.RegisteredComponent{registered}}
			var rt *druntime.Runtime
			if mode == "indexed" {
				rt, err = druntime.NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, caseLoader{registered})
				config.Components, config.Indexed, config.Loader = nil, []*spec.Component{artifact.Component}, rt
			} else {
				rt, err = druntime.NewRuntime(config.Components)
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			config.Invoker = rt
			service, err := mcp.New(config)
			require.NoError(t, err)
			require.NoError(t, service.PrepareTool(ctx, "Summary"))
			tool, ok := service.Registry().ToolRegistry.Get("Summary")
			require.True(t, ok)
			require.NotNil(t, tool.Metadata.OutputSchema)
			schemaBytes, err := json.Marshal(tool.Metadata.OutputSchema)
			require.NoError(t, err)
			var document map[string]any
			require.NoError(t, json.Unmarshal(schemaBytes, &document))
			properties := document["properties"].(map[string]any)
			require.Contains(t, properties, "status")
			require.Contains(t, properties, "error", "dynamic status payload remains representable")
			require.Contains(t, properties, "meta")
			require.Contains(t, properties, "rows")
			require.NotContains(t, properties, "Rows")
			for _, name := range []string{"pageCount", "recordCount", "advertiserId", "Exact_Name", "displayName"} {
				require.Contains(t, string(schemaBytes), `"`+name+`"`)
			}
			for _, name := range []string{"PageCount", "RecordCount", "AdvertiserId", "Hidden", "hidden", "excluded", "Ignored"} {
				require.NotContains(t, string(schemaBytes), `"`+name+`"`)
			}
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Summary"}})
			require.Nil(t, rpcErr)
			actual, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			const expected = `{"status":"ok","meta":{"pageCount":null,"recordCount":1},"rows":[{"advertiserId":7,"Exact_Name":"fixed","displayName":"label"}]}`
			require.JSONEq(t, expected, string(actual))
			res := httptest.NewRecorder()
			gateway.NewHandler(rt, nil, "test").ServeHTTP(res, httptest.NewRequest("GET", "/summary", nil))
			require.Equal(t, 200, res.Code, res.Body.String())
			require.JSONEq(t, expected, res.Body.String())
			validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schemaBytes), gojsonschema.NewBytesLoader(actual))
			require.NoError(t, err)
			require.True(t, validation.Valid(), "%v", validation.Errors())
		})
	}
}
