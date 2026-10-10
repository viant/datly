package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type boundInvoker struct{ calls int }

func (i *boundInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (interface{}, error) {
	i.calls++
	return map[string]interface{}{"query": "bound"}, nil
}

func TestOrdinaryComponentNativeWireAndLegacyPinRejection(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) { componentBindingNativeWire(t, version) })
	}
}

func componentBindingNativeWire(t *testing.T, version string) {
	ctx := context.Background()
	invoker := &boundInvoker{}
	artifact := &exec.LinkedArtifact{Revision: "artifact-release-1", ContentFingerprint: strings.Repeat("a", 64)}
	component := serviceComponent(t, "Search", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run"})
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, LinkedArtifact: artifact})
	if err != nil {
		t.Fatal(err)
	}
	native := mcpclient.New(t, service, version)
	listing, err := native.ListTools(ctx, nil)
	if err != nil || len(listing.Tools) != 1 {
		t.Fatalf("list: %+v %v", listing, err)
	}
	if _, exists := listing.Tools[0].Meta[tool.ComponentBindingMetaKey]; exists {
		t.Fatal("component identity published on MCP wire")
	}
	params := &schema.CallToolRequestParams{Name: "search.run", Arguments: map[string]interface{}{"query": "safe"}}
	if _, err := native.CallTool(ctx, params); err != nil {
		t.Fatal(err)
	}
	if invoker.calls != 1 {
		t.Fatalf("ordinary call not invoked: %d", invoker.calls)
	}
	for _, legacy := range []interface{}{nil, map[string]interface{}{"id": "old"}, "malformed"} {
		params.Meta.AdditionalProperties = map[string]interface{}{tool.ComponentBindingMetaKey: legacy}
		if _, err := native.CallTool(ctx, params); err == nil || !strings.Contains(err.Error(), "resource") {
			t.Fatalf("legacy pinned intent not rejected clearly: %v", err)
		}
		if invoker.calls != 1 {
			t.Fatal("legacy wire pin reached invoker")
		}
	}
}

func TestLegacyRequiredBindingAndMetadataSpoofFailClearly(t *testing.T) {
	component := serviceComponent(t, "Search", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run"})
	invoker := &boundInvoker{}
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, RequireComponentBinding: true})
	if err == nil || service != nil || !strings.Contains(err.Error(), "resource") {
		t.Fatal("legacy required-binding config accepted")
	}

	_, err = New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, LinkedArtifact: &exec.LinkedArtifact{Revision: "release-1", ContentFingerprint: strings.Repeat("a", 64)},
		ToolMetadata: func(context.Context, exec.ComponentTarget) (map[string]interface{}, error) {
			return map[string]interface{}{tool.ComponentBindingMetaKey: "forged"}, nil
		}})
	if err == nil {
		t.Fatal("legacy host metadata accepted on MCP wire")
	}
}
