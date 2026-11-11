package mcp_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type enumArgumentInput struct {
	Code  int    `parameter:",kind=query,in=code,required=true" choice:"0:draft" choice:"10:active" enumAccept:"literal"`
	Label string `parameter:",kind=body,in=label,required=true" choice:"0:draft" choice:"10:active" enumAccept:"ordinal"`
}

func TestMCPEnumScalarArgumentsConvertToOppositeStorageTypes(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "EnumArguments"}, Routes: []*spec.Route{{Method: "POST", Path: "/enum-args", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "EnumArguments"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[enumArgumentInput](), OutputType: reflect.TypeFor[enumArgumentInput]()})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *enumArgumentInput) (*enumArgumentInput, error) {
		calls++
		if in.Code != 10 || in.Label != "active" {
			t.Errorf("wrong storage values %+v", in)
		}
		return in, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.PrepareTool(t.Context(), "EnumArguments"); err != nil {
		t.Fatal(err)
	}
	tool, _ := service.Registry().ToolRegistry.Get("EnumArguments")
	if tool.Metadata.InputSchema.Properties["code"]["type"] != "string" || tool.Metadata.InputSchema.Properties["label"]["type"] != "integer" {
		t.Fatal(tool.Metadata.InputSchema)
	}
	for _, tc := range []struct {
		args  map[string]any
		valid bool
	}{{map[string]any{"code": "active", "label": 10}, true}, {map[string]any{"code": 10, "label": 10}, false}, {map[string]any{"code": "active", "label": "10"}, false}} {
		before := calls
		result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "EnumArguments", Arguments: tc.args}})
		failed := rpcErr != nil || result == nil || result.IsError != nil && *result.IsError
		if failed == tc.valid {
			t.Fatalf("args=%v result=%+v err=%v", tc.args, result, rpcErr)
		}
		if !tc.valid && calls != before {
			t.Fatal("invalid representation reached handler")
		}
	}
}
