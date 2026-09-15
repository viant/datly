package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSkillpackReferencesAndExactSync(t *testing.T) {
	source, destination := t.TempDir(), filepath.Join(t.TempDir(), "bundle")
	for _, folder := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		root := filepath.Join(source, folder)
		if err := os.MkdirAll(filepath.Join(root, "references"), 0755); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string]string{"SKILL.md": "---\nname: " + folder + "\ndescription: Example.\n---\n[read](references/guide.md)\n", "references/guide.md": "[local](other.md#supporting-content)", "references/other.md": "# Supporting content"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	p := packager{source: source, destination: destination, write: true}
	if err := p.run(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(destination, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Files                map[string]string
		UnresolvedReferences []string
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 9 || len(manifest.UnresolvedReferences) != 0 {
		t.Fatalf("manifest %+v", manifest)
	}
	original, _ := os.ReadFile(filepath.Join(source, "datly-reader/references/guide.md"))
	embedded, _ := os.ReadFile(filepath.Join(destination, "datly-reader/references/guide.md"))
	if !bytes.Equal(original, embedded) {
		t.Fatal("source links rewritten")
	}
	p.write = false
	if err = p.run(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(source, "datly-reader/SKILL.md"), []byte("[missing](references/missing.md)"), 0644); err != nil {
		t.Fatal(err)
	}
	if err = p.run(); err == nil || !strings.Contains(err.Error(), "unresolved document-relative link") {
		t.Fatalf("missing in-root link: %v", err)
	}
}
