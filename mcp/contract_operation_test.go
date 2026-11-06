package mcp_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
)

type operationBody struct {
	Name string               `json:"name"`
	Has  *struct{ Name bool } `json:"-" setMarker:"true"`
}

func (*operationBody) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Method string `parameter:",kind=contract,in=method,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	// Same name across routes proves route-scoped caches do not mix POST/PATCH.
	return &xcontract.Policy{Name: "operation", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[operationBody]() && f.Name == "Name" {
			if state.Method == "POST" {
				return f.Required(), nil
			}
			return f.Optional(), nil
		}
		return nil, nil
	}}, nil
}

type operationInput struct {
	Body   *operationBody `parameter:",kind=body,in=body,required=true" json:"body"`
	Method string         `parameter:",kind=http_request,in=method" json:"-"`
}
type operationOutput struct{ Method string }

func TestCreatePatchToolsUseTrustedOperationForDiscoveryAndInvocation(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "CampaignOperations"}, Routes: []*spec.Route{
		{Method: "POST", Path: "/campaigns", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "CreateCampaign"}}},
		{Method: "PATCH", Path: "/campaigns", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "PatchCampaign"}}},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[operationInput](), OutputType: reflect.TypeFor[operationOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	seen := ""
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *operationInput) (*operationOutput, error) {
		calls++
		seen = in.Method
		return &operationOutput{Method: in.Method}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: rt})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := mcpserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(t.Context(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*mcpserver.Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	list, rpcErr := handler.ListTools(t.Context(), nil)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if len(list.Tools) != 2 {
		t.Fatal("missing operation tool")
	}
	for _, tool := range list.Tools {
		required, _ := tool.InputSchema.Properties["body"]["required"].([]string)
		if (len(required) == 1 && required[0] == "name") != (tool.Name == "CreateCampaign") {
			t.Fatalf("%s required=%v", tool.Name, required)
		}
	}
	for _, tc := range []struct {
		name   string
		body   map[string]any
		valid  bool
		method string
	}{
		{"CreateCampaign", map[string]any{}, false, "POST"},
		{"PatchCampaign", map[string]any{}, true, "PATCH"},
		{"CreateCampaign", map[string]any{"name": "new"}, true, "POST"},
	} {
		before := calls
		call := &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: tc.name, Arguments: map[string]any{"body": tc.body, "Method": "DELETE"}}}
		result, rpcErr := handler.CallTool(t.Context(), &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: call})
		failed := rpcErr != nil || result == nil || result.IsError != nil && *result.IsError
		if failed == tc.valid {
			t.Fatalf("%s valid=%v error=%v result=%+v", tc.name, tc.valid, rpcErr, result)
		}
		if tc.valid && (calls != before+1 || seen != tc.method) {
			t.Fatal("tool name did not retain its declared method")
		}
		if !tc.valid && calls != before {
			t.Fatal("create requirement failed after handler execution")
		}
	}
	// OpenAPI is fetched with GET but describes POST and PATCH independently.
	scope, err := request.New(httptest.NewRequest("GET", "/openapi.json", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	doc, err := (openapi.Generator{}).Generate(t.Context(), openapi.Request{Info: openapi3.Info{Title: "operations", Version: "1"}, Components: []*registry.RegisteredComponent{registered}, Contracts: rt, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []struct {
		value    *openapi3.Operation
		required bool
	}{{doc.Paths["/campaigns"].Post, true}, {doc.Paths["/campaigns"].Patch, false}} {
		body := op.value.RequestBody.Content["application/json"].Schema
		if body.Ref != "" {
			body = doc.Components.Schemas[strings.TrimPrefix(body.Ref, "#/components/schemas/")]
		}
		// Named body binding places the contract under its authored member.
		if member := body.Properties["body"]; member != nil {
			body = member
		}
		if len(body.AnyOf) > 0 {
			body = body.AnyOf[0]
		}
		if body.Ref != "" {
			body = doc.Components.Schemas[strings.TrimPrefix(body.Ref, "#/components/schemas/")]
		}
		if (len(body.Required) == 1 && body.Required[0] == "name") != op.required {
			t.Fatalf("OpenAPI required=%v want=%v", body.Required, op.required)
		}
	}
}
