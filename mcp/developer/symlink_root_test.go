package developer_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness/devapp"
	"github.com/viant/datly/mcp/developer"
)

func TestTranscriptionSymlinkRootReportsRelativeFiles(t *testing.T) {
	fixture := devapp.New(t)
	config, err := devapp.Configuration(fixture.Root, fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(config.Targets["reader"].BaseDir)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "linked-project")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	target := config.Targets["reader"]
	target.BaseDir = alias
	config.Targets["reader"] = target
	service, err := developer.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(context.Background())
	result := call(t, service, developer.TranscribeTool, map[string]any{"target": "reader", "source": devapp.ReadDQL})
	if result.IsError != nil && *result.IsError {
		t.Fatalf("transcription: %+v", result.Content)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var report developer.Transcription
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Files) == 0 {
		t.Fatal("no generated files reported")
	}
	for _, name := range report.Files {
		if !fs.ValidPath(name) || filepath.IsAbs(name) {
			t.Fatalf("non-relative output provenance: %q", name)
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(name))); err != nil {
			t.Fatalf("reported file %q: %v", name, err)
		}
	}
}
