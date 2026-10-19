package mcp

import (
	"context"
	"reflect"
	"testing"
	"testing/fstest"

	resource "github.com/viant/datly/mcp/resource"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

func TestNamespaceResourceBridgeForwardsDeclaredPresentationTarget(t *testing.T) {
	service, err := New(Config{Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	target := map[string]interface{}{"platform": "web", "formFactor": "desktop", "capabilities": []string{"touch"}}
	service.Registry().RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "resource/get", InputSchema: schema.ToolInputSchema{Properties: map[string]map[string]interface{}{"target": {"type": "object"}}}}, Handler: func(_ context.Context, in *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		calls++
		if !reflect.DeepEqual(in.Params.Arguments["target"], target) {
			t.Fatalf("lost target: %+v", in.Params.Arguments)
		}
		return &schema.CallToolResult{StructuredContent: map[string]interface{}{"selected": "desktop"}}, nil
	}})
	if !service.SupportsWindowTarget() {
		t.Fatal("declared target not advertised")
	}
	request := &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "window://example/demo", Meta: schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{windowTargetExtension: target}}}}
	result, rpc := service.ReadResource(context.Background(), request)
	if rpc != nil || result == nil || calls != 1 {
		t.Fatalf("targeted read: %+v %v calls=%d", result, rpc, calls)
	}
	for _, invalid := range []interface{}{nil, "web", map[string]interface{}{"selectionToken": "forged"}, map[string]interface{}{"executionProof": map[string]interface{}{}}, map[string]interface{}{"dependencyPins": map[string]interface{}{}}, map[string]interface{}{"platform": 4}} {
		request.Params.Meta.AdditionalProperties = map[string]interface{}{windowTargetExtension: invalid}
		if result, rpc = service.ReadResource(context.Background(), request); result != nil || rpc == nil || calls != 1 {
			t.Fatalf("invalid target invoked resolver: %v", invalid)
		}
	}
	service.Registry().RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "resource/get"}})
	if service.SupportsWindowTarget() {
		t.Fatal("undeclared target advertised")
	}
	request.Params.Meta.AdditionalProperties = map[string]interface{}{windowTargetExtension: target}
	if result, rpc = service.ReadResource(context.Background(), request); result != nil || rpc == nil || calls != 1 {
		t.Fatal("unsupported target was ignored")
	}
}

func TestNamespaceResourceBridge(t *testing.T) {
	service, err := New(Config{Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	request := &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "navigation://management/main"}}
	if _, rpc := service.ReadResource(context.Background(), request); rpc == nil {
		t.Fatal("absent resolver changed missing resource behavior")
	}
	calls := 0
	failed := false
	service.Registry().RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "resource/get"}, Handler: func(ctx context.Context, in *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		calls++
		if in.Method != schema.MethodToolsCall || in.Params.Name != "resource/get" || in.Params.Arguments["uri"] != request.Params.Uri {
			t.Fatalf("incorrect resolver request: %+v", in)
		}
		return &schema.CallToolResult{StructuredContent: map[string]any{"id": "management"}, IsError: &failed}, nil
	}})
	result, rpc := service.ReadResource(context.Background(), request)
	if rpc != nil || result == nil || len(result.Contents) != 1 || result.Contents[0].Text != `{"id":"management"}` || result.Contents[0].Uri != request.Params.Uri || calls != 1 {
		t.Fatalf("incorrect bridge result: %+v %v calls=%d", result, rpc, calls)
	}
	if result.CacheScope != schema.ReadResourceResultCacheScopePrivate || result.TtlMs != 0 {
		t.Fatal("actor-scoped resource must not permit public or stale caching")
	}
	for _, uri := range []string{"window://management/namespaces", "datasource://management/namespaces", "mcp://opaque_namespace/connection"} {
		request.Params.Uri = uri
		if result, rpc = service.ReadResource(context.Background(), request); rpc != nil || result == nil || result.Contents[0].Uri != uri {
			t.Fatalf("resolver scheme %s: %+v %v", uri, result, rpc)
		}
	}
	failed = true
	if result, rpc = service.ReadResource(context.Background(), request); result != nil || rpc == nil {
		t.Fatal("tool error became successful resource contents")
	}
	service.authorizeResource = func(context.Context, string) error { return context.Canceled }
	before := calls
	if result, rpc = service.ReadResource(context.Background(), request); result != nil || rpc == nil || calls != before {
		t.Fatal("resource denial invoked resolver")
	}
}

func TestNamespaceResourceBridgeRetainsNativePriority(t *testing.T) {
	service, err := New(Config{Invoker: &serviceInvoker{}, Folders: []resource.Folder{{Namespace: "docs", URIPrefix: "docs://docs/", FS: fstest.MapFS{"readme.json": &fstest.MapFile{Data: []byte(`{"native":true}`)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	service.Registry().RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "resource/get"}, Handler: func(context.Context, *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		calls++
		return nil, jsonrpc.NewInternalError("must not call", nil)
	}})
	result, rpc := service.ReadResource(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "docs://docs/readme.json"}})
	if rpc != nil || result == nil || result.Contents[0].Text != `{"native":true}` {
		t.Fatalf("native resource lost: %+v %v", result, rpc)
	}
	_, _ = service.ReadResource(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "not a valid resource URI"}})
	if calls != 0 {
		t.Fatalf("native/malformed resource invoked resolver %d times", calls)
	}
}

func TestNamespaceResourceBridgeErrorClassification(t *testing.T) {
	service, err := New(Config{Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	request := &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "navigation://management/missing"}}
	for _, status := range []int{400, 401, 403, 404, 500} {
		failed := true
		service.Registry().RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "resource/get"}, Handler: func(context.Context, *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
			return &schema.CallToolResult{StructuredContent: map[string]any{"status": status}, IsError: &failed}, nil
		}})
		result, rpc := service.ReadResource(context.Background(), request)
		if result != nil || rpc == nil {
			t.Fatalf("status%d became contents", status)
		}
		if status == 404 && rpc.Code != schema.NewResourceNotFound(request.Params.Uri).Code {
			t.Fatalf("missing resource code=%d", rpc.Code)
		}
	}
}
