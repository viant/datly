package developer_test

import (
	"context"
	"github.com/viant/datly/internal/testharness/devapp"
	"github.com/viant/datly/mcp/developer"
	"github.com/viant/mcp-protocol/schema"
	"os"
	"path/filepath"
	"testing"
)

func TestDeveloperAuthorityFailuresAndCancellation(t *testing.T) {
	f := devapp.New(t)
	cfg, err := devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	request := cfg.Authoring["reader"]
	request.Destination = t.TempDir()
	cfg.Authoring["reader"] = request
	if _, err = developer.New(cfg); err == nil {
		t.Fatal("destination escape configured")
	}
	cfg, err = devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := developer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	for _, tc := range []struct {
		name string
		args map[string]any
	}{{developer.TranscribeTool, map[string]any{"target": "reader", "source": devapp.ReadDQL, "destination": "../escape"}}, {developer.RunTool, map[string]any{"target": "app", "port": 65535}}, {developer.RunTool, map[string]any{"target": "app", "address": "0.0.0.0"}}, {developer.StopTool, map[string]any{"instanceId": "not-owned"}}} {
		entry, _ := s.Registry().ToolRegistry.Get(tc.name)
		result, e := entry.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: tc.name, Arguments: tc.args}})
		if e == nil && (result.IsError == nil || !*result.IsError) {
			t.Fatalf("authority override accepted %s", tc.name)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entry, _ := s.Registry().ToolRegistry.Get(developer.TranscribeTool)
	result, e := entry.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: developer.TranscribeTool, Arguments: map[string]any{"target": "reader", "source": devapp.ReadDQL}}})
	if e == nil && !*result.IsError {
		t.Fatal("canceled write accepted")
	}
	if _, err = os.Stat(filepath.Join(f.Root, "reader/generated")); !os.IsNotExist(err) {
		t.Fatal("canceled transcription wrote files")
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	result = call(t, s, developer.ComponentsTool, map[string]any{"target": "app"})
	if !*result.IsError {
		t.Fatal("closed developer service admitted work")
	}
}
