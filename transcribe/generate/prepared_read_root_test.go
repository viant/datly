package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareCurrentAtUsesProjectedTreeAndNominalPaths(t *testing.T) {
	logical, projected := t.TempDir(), t.TempDir()
	put := func(root, name, content string) {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	put(logical, "input.go", "package sample\n// authored collision on real disk\n")
	put(projected, "input.go", generatedHeader("Sample")+"package sample\n")
	put(projected, "old_resources.go", generatedHeader("Sample")+"package sample\nimport \"embed\"\n//go:embed sql/retired.sql\nvar OldResources embed.FS\n")
	put(projected, "sql/retired.sql", "SELECT 1\n")
	makePersistence := func() *scaffoldPersistence {
		return &scaffoldPersistence{dir: logical, owner: "Sample", files: []EmittedFile{{Path: filepath.Join(logical, "input.go"), Content: "package sample\n"}}}
	}
	p := makePersistence()
	if err := p.prepareCurrentAt(logical, projected); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"old_resources.go", filepath.Join("sql", "retired.sql")} {
		if !p.renames[name] {
			t.Fatalf("missing projected retirement %s: %v", name, p.renames)
		}
	}
	if p.renames["input.go"] {
		t.Fatal("retired desired nominal file")
	}
	if err := makePersistence().prepareCurrent(logical); err == nil || !strings.Contains(err.Error(), "unowned") {
		t.Fatalf("ordinary wrapper must still inspect real destination: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(logical, "input.go"))
	if err != nil || !strings.Contains(string(content), "authored collision") {
		t.Fatalf("modified logical source: %q %v", content, err)
	}
}
