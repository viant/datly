package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/viant/datly/observability"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
)

func TestCallToolCarriesOnlyOptedInNumericTimingMetadata(t *testing.T) {
	registry := protocolserver.NewRegistry()
	failed := true
	registry.RegisterTool(&protocolserver.ToolEntry{
		Metadata: schema.Tool{Name: "timed"},
		Handler: func(ctx context.Context, _ *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
			finish := observability.StartToolTiming(ctx, observability.ToolTimingReaderInvoke)
			finish()
			return &schema.CallToolResult{IsError: &failed, Meta: &schema.ResultMetaObject{
				AdditionalProperties: map[string]interface{}{"existing": "retained"},
				IoModelcontextprotocolServerInfo: &schema.Implementation{Name: "existing", Version: "1"},
			}}, nil
		},
	})
	factory, err := NewHandler(&handlerService{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "timed"}}}
	ordinary, protocolErr := handler.CallTool(context.Background(), request)
	if protocolErr != nil || ordinary == nil || ordinary.Meta == nil || ordinary.IsError == nil || !*ordinary.IsError {
		t.Fatalf("ordinary error result=%#v protocolError=%v", ordinary, protocolErr)
	}
	ordinaryExtras, ok := ordinary.Meta.AdditionalProperties.(map[string]interface{})
	if !ok || ordinaryExtras[observability.ToolTimingMetaKey] != nil {
		t.Fatalf("ordinary call emitted timing: %#v", ordinary.Meta)
	}
	ctx := observability.WithToolTiming(context.Background())
	timed, protocolErr := handler.CallTool(ctx, request)
	if protocolErr != nil || timed == nil || timed.Meta == nil || timed.IsError == nil || !*timed.IsError {
		t.Fatalf("opted-in error result=%#v protocolError=%v", timed, protocolErr)
	}
	extras, ok := timed.Meta.AdditionalProperties.(map[string]interface{})
	if !ok || extras["existing"] != "retained" || timed.Meta.IoModelcontextprotocolServerInfo == nil || timed.Meta.IoModelcontextprotocolServerInfo.Name != "existing" {
		t.Fatalf("preexisting metadata lost: %#v", timed.Meta)
	}
	metrics, ok := extras[observability.ToolTimingMetaKey].(map[string]observability.ToolTimingMetric)
	if !ok || len(metrics) != 1 || metrics["reader_invoke"].Calls != 1 {
		t.Fatalf("unexpected numeric result metadata: %#v", extras)
	}
	wire, err := json.Marshal(timed)
	if err != nil {
		t.Fatal(err)
	}
	var decoded schema.CallToolResult
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Meta == nil {
		t.Fatal("MCP wire omitted result metadata")
	}
	decodedExtras, ok := decoded.Meta.AdditionalProperties.(map[string]interface{})
	if !ok || decoded.Meta.IoModelcontextprotocolServerInfo == nil || decoded.Meta.IoModelcontextprotocolServerInfo.Name != "existing" || decodedExtras["existing"] != "retained" {
		t.Fatalf("typed MCP metadata lost on wire: %#v", decoded.Meta)
	}
	metadata, ok := decodedExtras[observability.ToolTimingMetaKey].(map[string]interface{})
	if !ok || len(metadata) != 1 {
		t.Fatalf("MCP wire timing metadata = %#v", decoded.Meta)
	}
	metric, ok := metadata["reader_invoke"].(map[string]interface{})
	if !ok || metric["calls"] != float64(1) {
		t.Fatalf("MCP wire numeric metric = %#v", metadata["reader_invoke"])
	}
	if _, ok := metric["durationMs"].(float64); !ok {
		t.Fatalf("MCP wire duration is not numeric: %#v", metric["durationMs"])
	}
	if result := attachToolTiming(ctx, nil); result != nil {
		t.Fatalf("nil protocol result changed: %#v", result)
	}
	shared := &schema.CallToolResult{Meta: &schema.ResultMetaObject{AdditionalProperties: map[string]interface{}{"existing": "retained"}}}
	decorated := attachToolTiming(ctx, shared)
	sharedExtras := shared.Meta.AdditionalProperties.(map[string]interface{})
	if decorated == shared || decorated.Meta == shared.Meta || sharedExtras[observability.ToolTimingMetaKey] != nil {
		t.Fatalf("shared business result was modified: original=%#v decorated=%#v", shared, decorated)
	}
}
