package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func TestServiceExtraArgumentPolicyAndSchema(t *testing.T) {
	component := serviceComponent(t, "ExtraArgumentPolicy", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "records.search"})
	args := map[string]interface{}{"query": "scoped", "semanticSelection": map[string]interface{}{"entity": "records"}}
	for _, strict := range []bool{false, true} {
		invoker := &serviceInvoker{}
		service, err := New(Config{StrictArguments: strict, Components: []*registry.RegisteredComponent{component}, Invoker: invoker})
		if err != nil {
			t.Fatal(err)
		}
		entry, ok := service.Registry().ToolRegistry.Get("records.search")
		if !ok {
			t.Fatal("missing tool")
		}
		result, rpcErr := entry.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "records.search", Arguments: args}})
		if strict {
			if rpcErr == nil || !strings.Contains(rpcErr.Message, "unknown MCP argument") || result != nil {
				t.Fatalf("strict result=%v error=%v", result, rpcErr)
			}
			if invoker.request.Target.Component.Name != "" {
				t.Fatal("rejected arguments reached execution")
			}
		} else if rpcErr != nil || result == nil || len(invoker.request.Providers) != 1 {
			t.Fatalf("lenient result=%v error=%v", result, rpcErr)
		}
		raw, err := json.Marshal(entry.Metadata.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var vocabulary map[string]interface{}
		if err = json.Unmarshal(raw, &vocabulary); err != nil {
			t.Fatal(err)
		}
		if strict && vocabulary["additionalProperties"] != false {
			t.Fatalf("strict schema=%s", raw)
		}
		if !strict && vocabulary["additionalProperties"] == false {
			t.Fatalf("lenient schema=%s", raw)
		}
	}
	if len(args) != 2 {
		t.Fatal("caller arguments changed")
	}
}
