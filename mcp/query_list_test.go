package mcp

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/bootstrap"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"reflect"
	"testing"
)

func TestOptInQueryListsPreserveTypedMCPArrays(t *testing.T) {
	type input struct {
		IDs []int `parameter:",kind=query,in=id" queryList:"csv"`
	}
	type output struct {
		IDs []int `json:"ids"`
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/querylists", Name: "Lists"}, Routes: []*spec.Route{{Method: "GET", Path: "/lists", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Lists"}}}}, Parameters: []*spec.Parameter{{Name: "IDs", Source: spec.BindSource{Kind: "query", Name: "id"}, QueryListCSV: true, TypeExpr: "[]int"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[output](), Handler: customhandler.NewFunc(func(_ context.Context, in *input) (*output, error) { return &output{IDs: in.IDs}, nil })}
	service := runtimeToolService(t, registered)
	entry, ok := service.Registry().ToolRegistry.Get("Lists")
	if !ok {
		t.Fatal("tool")
	}
	result, protocolErr := entry.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Lists", Arguments: map[string]any{"IDs": []any{3, 1, 3}}}})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if result.IsError != nil && *result.IsError {
		t.Fatalf("tool error%+v", result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var out output
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.IDs, []int{3, 1, 3}) {
		t.Fatalf("MCP typed array changed%s", raw)
	}
}
