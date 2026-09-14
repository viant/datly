package developer_test

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness/devapp"
	"github.com/viant/datly/mcp/developer"
	"github.com/viant/datly/standalone"
	"github.com/viant/mcp-protocol/schema"
	"os"
	"path/filepath"
	"testing"
)

func call(t *testing.T, s *developer.Service, name string, args map[string]any) *schema.CallToolResult {
	t.Helper()
	entry, ok := s.Registry().ToolRegistry.Get(name)
	if !ok {
		t.Fatal("tool missing")
	}
	result, err := entry.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: name, Arguments: args}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestDeveloperTranscribeInspectAndProtection(t *testing.T) {
	f := devapp.New(t)
	config, err := devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := developer.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	before := call(t, s, developer.ComponentsTool, map[string]any{"target": "app"})
	if *before.IsError {
		t.Fatal(before.Content)
	}
	for _, test := range []struct{ target, source string }{{"reader", devapp.ReadDQL}, {"writer", devapp.WriteDQL}} {
		result := call(t, s, developer.TranscribeTool, map[string]any{"target": test.target, "source": test.source})
		if result.IsError != nil && *result.IsError {
			t.Fatalf("transcribe %s: %+v", test.target, result.Content)
		}
	}
	result := call(t, s, developer.ValidationTool, map[string]any{"target": "app"})
	if *result.IsError {
		t.Fatalf("validate: %+v", result.Content)
	}
	runtimeServer, err := standalone.New(context.Background(), config.Applications["app"].Options)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeServer.Shutdown(context.Background())
	if err = runtimeServer.Reload(context.Background(), 1); err != nil {
		t.Fatalf("transcribed runtime publication: %v", err)
	}
	listed := call(t, s, developer.ComponentsTool, map[string]any{"target": "app"})
	data, _ := json.Marshal(listed.StructuredContent)
	var list developer.ComponentList
	if err = json.Unmarshal(data, &list); err != nil || len(list.Components) != 2 {
		t.Fatalf("list=%s error=%v", data, err)
	}
	filtered := call(t, s, developer.ComponentsTool, map[string]any{"target": "app", "prefix": "component:example.com/mcpapp/reader"})
	if *filtered.IsError {
		t.Fatal(filtered.Content)
	}
	missing := call(t, s, developer.InspectTool, map[string]any{"target": "app", "component": "absent"})
	if !*missing.IsError {
		t.Fatal("absent component inspected")
	}
	inspected := call(t, s, developer.InspectTool, map[string]any{"target": "app", "component": list.Components[0].ID})
	if *inspected.IsError {
		t.Fatal(inspected.Content)
	}
	data, _ = json.Marshal(inspected.StructuredContent)
	var inspect developer.Inspection
	_ = json.Unmarshal(data, &inspect)
	if inspect.MetadataURI == "" {
		t.Fatal("metadata resource missing")
	}
	read, readErr := s.ReadResource(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: inspect.MetadataURI}})
	if readErr != nil || len(read.Contents) != 1 {
		t.Fatal(readErr)
	}
	path := filepath.Join(f.Root, "reader/generated/read_router.go")
	files, err := filepath.Glob(filepath.Join(f.Root, "reader/generated", "*router.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) > 0 {
		path = files[0]
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(data, []byte("\n// authored change\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	conflict := call(t, s, developer.TranscribeTool, map[string]any{"target": "reader", "source": devapp.ReadDQL})
	if conflict.IsError == nil || !*conflict.IsError {
		t.Fatal("authored artifact overwritten")
	}
}
