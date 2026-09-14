package mcp

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type lifecycleInput struct {
	Name        string `json:"name"`
	Initialized bool   `json:"-"`
	MCP         bool   `json:"-"`
}

func (i *lifecycleInput) Init(context.Context) error {
	i.Initialized = true
	return nil
}

func (i *lifecycleInput) InitMCP(context.Context, xmcp.Context) error {
	i.MCP = true
	return nil
}

type lifecycleOutput struct {
	Name         string `json:"name"`
	Initialized  bool   `json:"initialized"`
	MCP          bool   `json:"mcp"`
	Finalized    bool   `json:"finalized"`
	FinalizedMCP bool   `json:"finalizedMCP"`
}

func (o *lifecycleOutput) Finalize(context.Context) error {
	o.Finalized = true
	return nil
}

func (o *lifecycleOutput) FinalizeMCP(context.Context, xmcp.Context) error {
	o.FinalizedMCP = true
	return nil
}

func TestToolExecutesCustomHandlerThroughUnifiedRuntimeLifecycle(t *testing.T) {
	component, artifact := runtimeArtifact(t, "Custom", reflect.TypeOf(lifecycleInput{}), reflect.TypeOf(lifecycleOutput{}), "custom.run")
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(lifecycleOutput{}),
		Handler: customhandler.NewFunc[lifecycleInput, lifecycleOutput](func(_ context.Context, input *lifecycleInput) (*lifecycleOutput, error) {
			return &lifecycleOutput{Name: input.Name, Initialized: input.Initialized, MCP: input.MCP}, nil
		}),
	}
	result, _ := executeNativeRuntimeTool(t, registered, "custom.run", map[string]interface{}{"name": "Ada"}, schema.LatestProtocolVersion)
	for _, name := range []string{"initialized", "mcp", "finalized", "finalizedMCP"} {
		if testharness.StructuredObject(t, result.StructuredContent)[name] != true {
			t.Fatalf("custom lifecycle result = %+v", result.StructuredContent)
		}
	}
}

func TestResourceExecutesCustomHandlerThroughUnifiedRuntimeLifecycle(t *testing.T) {
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Resource"}, Name: "Resource",
		Routes: []*spec.Route{{Method: "GET", Path: "/resources/{name}", MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResourceTemplate, Name: "resources", MIMEType: "application/json",
		}}}},
		Parameters: []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "path", Name: "name"}, Required: &required}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(lifecycleInput{}), OutputType: reflect.TypeOf(lifecycleOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(lifecycleOutput{}),
		Handler: customhandler.NewFunc[lifecycleInput, lifecycleOutput](func(_ context.Context, input *lifecycleInput) (*lifecycleOutput, error) {
			return &lifecycleOutput{Name: input.Name, Initialized: input.Initialized, MCP: input.MCP}, nil
		}),
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	result, protocolErr := service.ReadResource(context.Background(), &schema.ReadResourceRequest{
		Method: schema.MethodResourcesRead,
		Params: schema.ReadResourceRequestParams{Uri: "datly://localhost/resources/Ada"},
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	content := result.Contents[0]
	actual := &lifecycleOutput{}
	if err := json.Unmarshal([]byte(content.Text), actual); err != nil {
		t.Fatal(err)
	}
	if actual.Name != "Ada" || !actual.Initialized || !actual.MCP || !actual.Finalized || !actual.FinalizedMCP {
		t.Fatalf("resource output = %+v", actual)
	}
}

type veltyInput struct {
	Name string `json:"name"`
}

type veltyOutput struct {
	Greeting string `json:"greeting"`
}

func TestToolExecutesVeltyHandlerThroughUnifiedRuntime(t *testing.T) {
	component, artifact := runtimeArtifact(t, "Velty", reflect.TypeOf(veltyInput{}), reflect.TypeOf(veltyOutput{}), "velty.run")
	handler, err := veltyhandler.New[veltyInput, veltyOutput](veltyhandler.Config{Template: `#set($Output.Greeting = $Input.Name)`})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := executeNativeRuntimeTool(t, &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(veltyOutput{}), Handler: handler,
	}, "velty.run", map[string]interface{}{"name": "hello"}, schema.LatestProtocolVersion)
	if testharness.StructuredObject(t, result.StructuredContent)["greeting"] != "hello" {
		t.Fatalf("Velty result = %+v", result.StructuredContent)
	}
}

func runtimeArtifact(t *testing.T, name string, inputType, outputType reflect.Type, toolName string) (*spec.Component, *bootstrap.Artifact) {
	t.Helper()
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: name}, Name: name,
		Routes:     []*spec.Route{{Method: "POST", Path: "/tools/" + name, MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: toolName}}}},
		Parameters: []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "body", Name: "name"}, Required: &required}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: outputType})
	if err != nil {
		t.Fatal(err)
	}
	return component, artifact
}

func executeRuntimeTool(t *testing.T, registered *registry.RegisteredComponent, name string, arguments map[string]interface{}) *schema.CallToolResult {
	return executeRuntimeToolContext(t, context.Background(), registered, name, arguments)
}

func executeRuntimeToolContext(t *testing.T, ctx context.Context, registered *registry.RegisteredComponent, name string, arguments map[string]interface{}) *schema.CallToolResult {
	t.Helper()
	service := runtimeToolService(t, registered)
	entry, ok := service.Registry().ToolRegistry.Get(name)
	if !ok {
		t.Fatalf("tool %q was not registered", name)
	}
	result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{
		Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: name, Arguments: arguments},
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if result.IsError != nil && *result.IsError {
		t.Fatalf("tool result error = %+v", result)
	}
	return result
}

func runtimeToolService(t *testing.T, registered *registry.RegisteredComponent) *Service {
	t.Helper()
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func executeNativeRuntimeTool(t *testing.T, registered *registry.RegisteredComponent, name string, arguments map[string]any, protocolVersion string) (*schema.CallToolResult, *schema.ToolInputSchema) {
	t.Helper()
	native := mcpclient.New(t, runtimeToolService(t, registered), protocolVersion)
	listed, err := native.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var inputSchema *schema.ToolInputSchema
	for _, tool := range listed.Tools {
		if tool.Name == name {
			copy := tool.InputSchema
			inputSchema = &copy
			break
		}
	}
	if inputSchema == nil {
		t.Fatalf("tool %q missing from wire listing", name)
	}
	result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: name, Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.IsError != nil && *result.IsError {
		t.Fatalf("native MCP error result=%+v", result)
	}
	return result, inputSchema
}
