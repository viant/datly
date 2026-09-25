package mcp

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestToolMetadataIsHostOwnedAndPreservesToolContract(t *testing.T) {
	component := serviceComponent(t, "Search", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run"})
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &serviceInvoker{},
		ToolMetadata: func(_ context.Context, target exec.ComponentTarget) (map[string]interface{}, error) {
			if target.Component.Name != "Search" {
				t.Fatalf("wrong component target: %+v", target)
			}
			return map[string]interface{}{ToolSourceMetaKey: ToolSourceIdentity{ReportID: "source-report", Version: 3}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := service.Registry().ToolRegistry.Get("search.run")
	if !ok || entry.Metadata.Name != "search.run" || entry.Metadata.InputSchema.Type != "object" {
		t.Fatalf("tool contract changed: %+v", entry)
	}
	metadata, ok := entry.Metadata.Meta[ToolSourceMetaKey].(ToolSourceIdentity)
	if !ok || metadata.ReportID != "source-report" || metadata.Version != 3 {
		t.Fatalf("host-owned source identity missing: %+v", entry.Metadata.Meta)
	}
	if service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &serviceInvoker{},
		ToolMetadata: func(context.Context, exec.ComponentTarget) (map[string]interface{}, error) {
			return nil, errors.New("missing version")
		},
	}); err == nil || service != nil {
		t.Fatalf("host metadata failure published a tool: %+v %v", service, err)
	}
}
