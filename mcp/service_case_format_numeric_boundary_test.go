package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

type numericBoundaryChild struct {
	ChannelV2Id int
	Fixed       int    `json:"ChannelV2ID"`
	Secret      string `json:"-" internal:"true"`
}
type numericBoundaryBody struct{ Children []numericBoundaryChild }
type numericBoundaryInput struct {
	Body *numericBoundaryBody `parameter:",kind=body,required=true" anonymous:"true"`
}
type numericBoundaryOutput struct{ Children []numericBoundaryChild }

func TestMCPCaseFormatNumericBoundaryGraph(t *testing.T) {
	ctx := context.Background()
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "NumericGraph"}, Routes: []*spec.Route{{Method: "POST", Path: "/numeric-graph", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "NumericGraph"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[numericBoundaryInput](), OutputType: reflect.TypeFor[numericBoundaryOutput]()})
	require.NoError(t, err)
	var bound *numericBoundaryBody
	registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[numericBoundaryInput, numericBoundaryOutput](func(_ context.Context, in *numericBoundaryInput) (*numericBoundaryOutput, error) {
		bound = in.Body
		return &numericBoundaryOutput{Children: in.Body.Children}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(ctx, "NumericGraph"))
	tool, ok := service.Registry().ToolRegistry.Get("NumericGraph")
	require.True(t, ok)
	items := tool.Metadata.InputSchema.Properties["children"]["items"].(map[string]any)
	require.Contains(t, items["properties"], "channelV2Id")
	require.Contains(t, items["properties"], "ChannelV2ID", "explicit JSON name must remain exact")
	require.NotContains(t, items["properties"], "secret")
	result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "NumericGraph", Arguments: map[string]any{"children": []any{map[string]any{"channelV2Id": 7, "ChannelV2ID": 9}}}}})
	require.Nil(t, rpcErr)
	require.False(t, result.IsError != nil && *result.IsError)
	require.NotNil(t, bound)
	require.Equal(t, []numericBoundaryChild{{ChannelV2Id: 7, Fixed: 9}}, bound.Children)
	actual, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.JSONEq(t, `{"children":[{"channelV2Id":7,"ChannelV2ID":9}]}`, string(actual))
	outputSchema, err := json.Marshal(tool.Metadata.OutputSchema)
	require.NoError(t, err)
	validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(outputSchema), gojsonschema.NewBytesLoader(actual))
	require.NoError(t, err)
	require.True(t, validation.Valid(), "%v", validation.Errors())
}

type numericBoundaryRow struct {
	ChannelV2ID int    `sqlx:"channel_v2_id"`
	Fixed       string `sqlx:"fixed" json:"Exact_Name"`
	Secret      string `sqlx:"secret" internal:"true" json:"-"`
}
type numericBoundaryRows struct {
	Data []numericBoundaryRow `parameter:",kind=output,in=view"`
}

func TestMCPCaseFormatNumericBoundarySelectors(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(channel_v2_id INTEGER,fixed TEXT,secret TEXT)", "INSERT INTO records VALUES(2,'two','hidden'),(1,'one','hidden')"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "NumericSelectors"}, Routes: []*spec.Route{{Method: "GET", Path: "/numeric-selectors", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "NumericSelectors"}}}}, RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"channel_v2_id"}}, Source: &spec.ViewSource{SQL: "SELECT channel_v2_id,fixed FROM records"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[publicNamesInput](), OutputType: reflect.TypeFor[numericBoundaryRows](), DirectViewField: "Data"})
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
	require.NoError(t, service.PrepareTool(ctx, "NumericSelectors"))
	tool, ok := service.Registry().ToolRegistry.Get("NumericSelectors")
	require.True(t, ok)
	for _, tc := range []struct {
		name           string
		fields         []string
		sort, expected string
		bad            bool
	}{
		{"numeric public field", []string{"channelV2Id"}, "channelV2Id ASC", `{"data":[{"channelV2Id":1},{"channelV2Id":2}]}`, false},
		{"explicit name", []string{"Exact_Name"}, "channelV2Id DESC", `{"data":[{"Exact_Name":"two"},{"Exact_Name":"one"}]}`, false},
		{"hidden", []string{"secret"}, "", "", true},
		{"forbidden sort", []string{"Exact_Name"}, "Exact_Name DESC", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "NumericSelectors", Arguments: map[string]any{"fields": tc.fields, "orderBy": tc.sort}}})
			if tc.bad {
				require.True(t, rpcErr != nil || result != nil && result.IsError != nil && *result.IsError)
				return
			}
			require.Nil(t, rpcErr)
			require.False(t, result.IsError != nil && *result.IsError)
			actual, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(actual))
		})
	}
}
