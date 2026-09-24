package mcp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

type mcpScopeContextInput struct {
	Credential string `parameter:"Credential,kind=header,in=Authorization"`
}

type mcpScopeContextOutput struct {
	Tenant int      `json:"tenant"`
	IDs    []string `json:"ids"`
}

type mcpScopedInput struct {
	Scope     *mcpScopeContextOutput `parameter:"Scope,kind=component,in=GET:/scope" json:"-"`
	Tenant    int                    `parameter:"Tenant,kind=param,in=Scope.Tenant,required=true" json:"-"`
	IDs       []string               `parameter:"IDs,kind=param,in=Scope.IDs,required=true" json:"-"`
	Requested string                 `parameter:"Requested,kind=body,in=requested" json:"requested"`
}

type mcpScopedOutput struct {
	Tenant    int      `json:"tenant"`
	IDs       []string `json:"ids"`
	Requested string   `json:"requested"`
}

// TestToolArgumentsCannotOverrideComponentBoundScope proves the MCP path uses
// the same native binding as HTTP: the consumer's component-kind and
// param-kind inputs come only from the trusted context component invoked with
// the caller's credential, tool arguments of the same names are ignored, and a
// denying context component denies the tool.
func TestToolArgumentsCannotOverrideComponentBoundScope(t *testing.T) {
	tenants := map[string]mcpScopeContextOutput{"Bearer alice": {Tenant: 1, IDs: []string{"101", "102"}}, "Bearer bob": {Tenant: 2, IDs: []string{"103"}}}
	contextComponent := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Context"}, Name: "Context",
		Routes: []*spec.Route{{Method: "GET", Path: "/scope"}},
	}
	contextArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contextComponent, InputType: reflect.TypeOf(mcpScopeContextInput{}), OutputType: reflect.TypeOf(mcpScopeContextOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	contextRegistration := &registry.RegisteredComponent{Component: contextArtifact.Component, Input: contextArtifact.Input, OutputType: reflect.TypeOf(mcpScopeContextOutput{}), Handler: customhandler.NewFunc[mcpScopeContextInput, mcpScopeContextOutput](func(_ context.Context, input *mcpScopeContextInput) (*mcpScopeContextOutput, error) {
		decision, ok := tenants[input.Credential]
		if !ok {
			return nil, &response.Error{Code: 403, Cause: errors.New("unknown principal")}
		}
		return &decision, nil
	})}
	required := true
	consumer := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Scoped"}, Name: "Scoped",
		Routes: []*spec.Route{{Method: "POST", Path: "/tools/Scoped", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "scoped.run"}}}},
		Parameters: []*spec.Parameter{
			{Name: "Scope", Source: spec.BindSource{Kind: "component", Name: "GET:/scope"}, Required: &required},
			{Name: "Tenant", TypeExpr: "int", Source: spec.BindSource{Kind: "param", Name: "Scope.Tenant"}, Required: &required},
			{Name: "IDs", TypeExpr: "[]string", Source: spec.BindSource{Kind: "param", Name: "Scope.IDs"}, Required: &required},
			{Name: "Requested", Source: spec.BindSource{Kind: "body", Name: "requested"}},
		},
	}
	consumerArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: consumer, InputType: reflect.TypeOf(mcpScopedInput{}), OutputType: reflect.TypeOf(mcpScopedOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	consumerRegistration := &registry.RegisteredComponent{Component: consumerArtifact.Component, Input: consumerArtifact.Input, OutputType: reflect.TypeOf(mcpScopedOutput{}), Handler: customhandler.NewFunc[mcpScopedInput, mcpScopedOutput](func(_ context.Context, input *mcpScopedInput) (*mcpScopedOutput, error) {
		return &mcpScopedOutput{Tenant: input.Tenant, IDs: input.IDs, Requested: input.Requested}, nil
	})}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{contextRegistration, consumerRegistration})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	service, err := New(Config{Components: []*registry.RegisteredComponent{contextRegistration, consumerRegistration}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := service.Registry().ToolRegistry.Get("scoped.run")
	if !ok {
		t.Fatal("tool scoped.run was not registered")
	}
	call := func(token string, arguments map[string]any) (*schema.CallToolResult, error) {
		ctx := context.Background()
		if token != "" {
			ctx = context.WithValue(ctx, authorization.TokenKey, &authorization.Token{Token: token})
		}
		result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "scoped.run", Arguments: arguments}})
		if protocolErr != nil {
			return nil, errors.New(protocolErr.Message)
		}
		if result.IsError != nil && *result.IsError {
			return result, errors.New("tool error")
		}
		return result, nil
	}
	malicious := map[string]any{"requested": "x", "Tenant": 9, "tenant": 9, "IDs": []string{"999"}, "ids": []string{"999"}, "Scope": map[string]any{"tenant": 9, "ids": []string{"999"}}, "scope": map[string]any{"tenant": 9}}

	t.Run("bound context reaches the tool", func(t *testing.T) {
		result, err := call("alice", map[string]any{"requested": "x"})
		if err != nil {
			t.Fatal(err)
		}
		structured := testharness.StructuredObject(t, result.StructuredContent)
		if structured["tenant"] != float64(1) || structured["requested"] != "x" {
			t.Fatalf("structured=%+v", structured)
		}
	})
	t.Run("distinct principals receive distinct scope", func(t *testing.T) {
		result, err := call("bob", map[string]any{"requested": "x"})
		if err != nil {
			t.Fatal(err)
		}
		structured := testharness.StructuredObject(t, result.StructuredContent)
		if structured["tenant"] != float64(2) {
			t.Fatalf("structured=%+v", structured)
		}
	})
	t.Run("arguments cannot supply or override bound kinds", func(t *testing.T) {
		result, err := call("alice", malicious)
		if err != nil {
			// Rejecting unknown arguments is acceptable; widening is not.
			if result != nil && strings.Contains(textContent(result), "999") {
				t.Fatalf("rejected call leaked forged scope: %+v", result)
			}
			return
		}
		structured := testharness.StructuredObject(t, result.StructuredContent)
		if structured["tenant"] != float64(1) || strings.Contains(textContent(result), "999") {
			t.Fatalf("forged arguments widened scope: %+v", structured)
		}
	})
	t.Run("denied context component denies the tool", func(t *testing.T) {
		result, err := call("mallory", map[string]any{"requested": "x"})
		if err == nil {
			t.Fatalf("unknown principal executed the tool: %+v", result)
		}
	})
	t.Run("missing credential denies the tool", func(t *testing.T) {
		result, err := call("", malicious)
		if err == nil {
			t.Fatalf("missing credential executed the tool: %+v", result)
		}
	})
}

func textContent(result *schema.CallToolResult) string {
	if result == nil {
		return ""
	}
	var parts []string
	for _, item := range result.Content {
		if text, ok := item.(schema.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}
