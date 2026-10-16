package project

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRequiresRoot(t *testing.T) {
	for _, root := range []string{"", " \t"} {
		if _, err := NewStore(root); err == nil {
			t.Fatalf("accepted empty project root %q", root)
		}
	}
}

func TestStoreRejectsSnapshotEscape(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Persist(&Manifest{Version: ManifestVersion}, map[string]any{"../outside.json": true}); err == nil {
		t.Fatal("accepted snapshot outside metadata directory")
	}
	for _, path := range []string{filepath.Join(root, "outside.json"), filepath.Join(root, MetadataDir, ManifestFile)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed persistence wrote %s: %v", path, err)
		}
	}
}
