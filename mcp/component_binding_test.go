package mcp

import (
	"context"
	"encoding/json"
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

func TestComponentBindingNativeWireAndNoFallback(t *testing.T) {
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
	raw, err := json.Marshal(listing.Tools[0].Meta[tool.ComponentBindingMetaKey])
	if err != nil {
		t.Fatal(err)
	}
	var binding exec.ComponentBinding
	if err := json.Unmarshal(raw, &binding); err != nil || binding.Validate() != nil {
		t.Fatalf("binding=%s %v", raw, err)
	}
	if binding.ID != component.Component.Key.String() || binding.Revision != artifact.Revision {
		t.Fatalf("wrong actual source %+v", binding)
	}
	// Caller mutation of the transferred declaration cannot change publication.
	artifact.Revision = "changed-after-publication"
	call := func(pin interface{}) error {
		params := &schema.CallToolRequestParams{Name: "search.run", Arguments: map[string]interface{}{"query": "safe"}}
		if pin != nil {
			params.Meta.AdditionalProperties = map[string]interface{}{tool.ComponentBindingMetaKey: pin}
		}
		_, err := native.CallTool(ctx, params)
		return err
	}
	if err := call(binding); err != nil {
		t.Fatal(err)
	}
	if invoker.calls != 1 {
		t.Fatalf("exact binding not invoked: %d", invoker.calls)
	}
	for _, field := range []string{"missing", "revision", "id", "content", "schema", "kind", "malformed"} {
		t.Run(field, func(t *testing.T) {
			pin := binding
			var expected interface{} = pin
			switch field {
			case "missing":
				expected = nil
			case "revision":
				pin.Revision = "unavailable-historical-artifact"
			case "id":
				pin.ID = "other-component"
			case "content":
				pin.ContentFingerprint = strings.Repeat("b", 64)
			case "schema":
				pin.SchemaFingerprint = strings.Repeat("c", 64)
			case "kind":
				pin.Kind = "dynamic"
			case "malformed":
				expected = map[string]interface{}{"id": pin.ID}
			}
			if field != "missing" && field != "malformed" {
				expected = pin
			}
			if err := call(expected); err == nil {
				t.Fatal("mismatched binding dispatched")
			}
			if invoker.calls != 1 {
				t.Fatalf("denied call reached native invoker: %d", invoker.calls)
			}
		})
	}
}

func TestRequiredBindingWithUnknownArtifactAndMetadataSpoofFailsClosed(t *testing.T) {
	component := serviceComponent(t, "Search", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run"})
	invoker := &boundInvoker{}
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, RequireComponentBinding: true})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := service.Registry().ToolRegistry.Get("search.run")
	_, protocolErr := entry.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "search.run", Arguments: map[string]interface{}{"query": "safe"}}})
	if protocolErr == nil || invoker.calls != 0 {
		t.Fatal("unknown artifact executed")
	}
	_, err = New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, LinkedArtifact: &exec.LinkedArtifact{Revision: "release-1", ContentFingerprint: strings.Repeat("a", 64)},
		ToolMetadata: func(context.Context, exec.ComponentTarget) (map[string]interface{}, error) {
			return map[string]interface{}{tool.ComponentBindingMetaKey: "forged"}, nil
		}})
	if err == nil {
		t.Fatal("host generic metadata replaced native binding")
	}
}
