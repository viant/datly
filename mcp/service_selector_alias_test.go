package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
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
	xcodec "github.com/viant/xdatly/codec"
)

type aliasChannelsRow struct {
	ID                    int      `sqlx:"id" json:"id"`
	AdOrderChannelsV2Mask []string `sqlx:"ad_order_channels_v2_mask,type=*int" codec:"CampaignChannels" json:"adOrderChannelsV2" selectorAlias:"adOrderChannelsV2"`
	Label                 string   `sqlx:"label" json:"publicLabel"`
}

type aliasChannelsOutput struct {
	Data []aliasChannelsRow `parameter:",kind=output,in=view" json:"data"`
}

type aliasChannelsCodec struct{}

func (aliasChannelsCodec) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if config.Body != "CampaignChannels" || config.SourceType != reflect.TypeFor[*int]() || config.DestinationType != reflect.TypeFor[[]string]() {
		return nil, fmt.Errorf("unexpected channels codec contract: %+v", config)
	}
	return aliasChannelsCodec{}, nil
}

func (aliasChannelsCodec) Value(_ context.Context, raw any, _ ...xcodec.Option) (any, error) {
	mask, ok := raw.(*int)
	if !ok {
		return nil, fmt.Errorf("unexpected channels mask %T", raw)
	}
	result := []string{}
	if mask != nil && *mask&1 != 0 {
		result = append(result, "Display")
	}
	return result, nil
}

func TestExplicitSelectorAliasCodecHTTPMCP(t *testing.T) {
	for _, mode := range []string{"eager", "indexed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE records(id INTEGER, ad_order_channels_v2_mask INTEGER, label TEXT)",
				"INSERT INTO records VALUES (1, 1, 'first'), (2, 0, 'second')",
			))
			component := &spec.Component{
				Key:      spec.Key{Kind: spec.KindComponent, Name: "Channels"},
				Routes:   []*spec.Route{{Method: "GET", Path: "/channels", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Channels"}}}},
				RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id, ad_order_channels_v2_mask, label FROM records ORDER BY id"}},
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
				Component: component, InputType: reflect.TypeFor[caseInput](), OutputType: reflect.TypeFor[aliasChannelsOutput](), DirectViewField: "Data", CodecFactory: aliasChannelsCodec{},
			})
			require.NoError(t, err)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
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
			require.NoError(t, service.PrepareTool(ctx, "Channels"))
			tool, ok := service.Registry().ToolRegistry.Get("Channels")
			require.True(t, ok)
			handler := gateway.NewHandler(rt, nil, "test")
			for _, tc := range []struct {
				name   string
				fields []string
				want   string
			}{
				{"public alias", []string{"adOrderChannelsV2"}, `{"data":[{"adOrderChannelsV2":["Display"]},{"adOrderChannelsV2":[]}]}`},
				{"SQL name", []string{"ad_order_channels_v2_mask"}, `{"data":[{"adOrderChannelsV2":["Display"]},{"adOrderChannelsV2":[]}]}`},
				{"unselected codec", []string{"id"}, `{"data":[{"id":1},{"id":2}]}`},
				{"full after projection", nil, `{"data":[{"id":1,"adOrderChannelsV2":["Display"],"publicLabel":"first"},{"id":2,"adOrderChannelsV2":[],"publicLabel":"second"}]}`},
				{"unknown", []string{"missing"}, ""},
				{"JSON name is not an alias", []string{"publicLabel"}, ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					res := httptest.NewRecorder()
					handler.ServeHTTP(res, httptest.NewRequest("GET", "/channels?"+url.Values{"fields": tc.fields}.Encode(), nil))
					args := map[string]any{}
					if tc.fields != nil {
						args["Fields"] = tc.fields
					}
					result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Channels", Arguments: args}})
					if tc.want == "" {
						require.NotEqual(t, 200, res.Code)
						require.True(t, rpcErr != nil || (result != nil && result.IsError != nil && *result.IsError))
						return
					}
					require.Equal(t, 200, res.Code, res.Body.String())
					require.JSONEq(t, tc.want, res.Body.String())
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
