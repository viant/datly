package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

type caseInput struct {
	Fields []string `parameter:"Fields,kind=query,in=fields" querySelector:"records"`
}

type caseRow struct {
	CandidateId     string  `sqlx:"candidate_id"`
	Nullable        *string `sqlx:"nullable"`
	Zero            int     `sqlx:"zero"`
	Enabled         bool    `sqlx:"enabled"`
	Empty           string  `sqlx:"empty"`
	SampleSeen_1Day int     `sqlx:"day"`
	SampleSeen_7Day int     `sqlx:"week"`
}

type caseOutput struct {
	response.Status
	Data    []caseRow `parameter:",kind=output,in=view"`
	Metrics response.Metrics
}

func (o *caseOutput) Finalize(context.Context) error {
	o.Status = response.Status{Status: "ok"}
	o.Metrics = response.Metrics{}
	return nil
}

type caseLoader struct{ registered *druntime.RegisteredComponent }

func (l caseLoader) LoadComponent(ctx context.Context, key spec.Key) (*druntime.RegisteredComponent, error) {
	return l.registered, ctx.Err()
}

func TestNativeCaseFormatHTTPMCP(t *testing.T) {
	for _, mode := range []string{"eager", "indexed", "implicit output contract"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE records(candidate_id TEXT, nullable TEXT, zero INTEGER, enabled BOOLEAN, empty TEXT, day INTEGER, week INTEGER)",
				"INSERT INTO records VALUES ('public', NULL, 0, FALSE, '', 11, 77)",
			))
			component := &spec.Component{
				Key:      spec.Key{Kind: spec.KindComponent, Name: "Records"},
				Settings: &spec.Settings{CaseFormat: "lc"},
				Routes:   []*spec.Route{{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Records"}}}},
				RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT candidate_id, nullable, zero, enabled, empty, day, week FROM records"}},
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[caseInput](), OutputType: reflect.TypeFor[caseOutput](), DirectViewField: "Data"})
			require.NoError(t, err)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			if mode == "implicit output contract" {
				registered.Output = nil
			}
			var rt *druntime.Runtime
			config := mcp.Config{Components: []*druntime.RegisteredComponent{registered}}
			if mode == "indexed" {
				rt, err = druntime.NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, caseLoader{registered})
				config.Components = nil
				config.Indexed = []*spec.Component{artifact.Component}
				config.Loader = rt
			} else {
				rt, err = druntime.NewRuntime(config.Components)
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			config.Invoker = rt
			service, err := mcp.New(config)
			require.NoError(t, err)
			require.NoError(t, service.PrepareTool(ctx, "Records"))
			tool, ok := service.Registry().ToolRegistry.Get("Records")
			require.True(t, ok)
			http := gateway.NewHandler(rt, nil, "test")
			for _, tc := range []struct {
				name   string
				fields []string
				want   string
			}{
				{"full", nil, `{"status":"ok","data":[{"candidateId":"public","nullable":null,"zero":0,"enabled":false,"empty":"","sampleSeen_Day":77}],"metrics":[]}`},
				{"projected", []string{"candidate_id", "nullable", "zero", "enabled", "empty"}, `{"status":"ok","data":[{"candidateId":"public","nullable":null,"zero":0,"enabled":false,"empty":""}],"metrics":[]}`},
				{"day", []string{"day"}, `{"status":"ok","data":[{"sampleSeen_Day":11}],"metrics":[]}`},
				{"week", []string{"week"}, `{"status":"ok","data":[{"sampleSeen_Day":77}],"metrics":[]}`},
				{"full after projection", nil, `{"status":"ok","data":[{"candidateId":"public","nullable":null,"zero":0,"enabled":false,"empty":"","sampleSeen_Day":77}],"metrics":[]}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					args := map[string]any{}
					if tc.fields != nil {
						args["Fields"] = tc.fields
					}
					result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Records", Arguments: args}})
					require.Nil(t, rpcErr)
					require.NotNil(t, result)
					if result.IsError != nil {
						require.False(t, *result.IsError, "%+v", result)
					}
					body, err := json.Marshal(result.StructuredContent)
					require.NoError(t, err)
					require.JSONEq(t, tc.want, string(body))
					text := result.Content[0].(schema.TextContent).Text
					require.JSONEq(t, tc.want, text)
					res := httptest.NewRecorder()
					http.ServeHTTP(res, httptest.NewRequest("GET", "/records?"+url.Values{"fields": tc.fields}.Encode(), nil))
					require.Equal(t, 200, res.Code, res.Body.String())
					require.JSONEq(t, tc.want, res.Body.String())
					if tc.name == "day" || tc.name == "week" {
						require.Equal(t, 1, strings.Count(text, `"sampleSeen_Day":`))
						require.Equal(t, 1, strings.Count(res.Body.String(), `"sampleSeen_Day":`))
					}
				})
			}
		})
	}
}
