package testharness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedModuleHonorsSelectedModfile(t *testing.T) {
	root := t.TempDir()
	original := "module github.com/viant/datly\ngo 1.25\n"
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(t.TempDir(), "selected.mod")
	if err := os.WriteFile(selected, []byte(original+"replace example.com/dependency => ../local\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATLY_TEST_MODFILE", selected)
	content, err := (GeneratedModule{SourceRoot: root}).Content()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "example.com/dependency") {
		t.Fatal("selected graph was lost")
	}
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || string(raw) != original {
		t.Fatal("source go.mod changed")
	}
	t.Setenv("DATLY_TEST_MODFILE", "relative.mod")
	if _, err = (GeneratedModule{SourceRoot: root}).Content(); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("got %v", err)
	}
	if _, err = (GeneratedModule{SourceRoot: root, SourceModFile: selected}).Content(); err != nil {
		t.Fatal("explicit source selection did not win", err)
	}
	bad := filepath.Join(t.TempDir(), "wrong.mod")
	if err = os.WriteFile(bad, []byte("module example.com/other\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATLY_TEST_MODFILE", bad)
	if _, err = (GeneratedModule{SourceRoot: root}).Content(); err == nil || !strings.Contains(err.Error(), "must declare source module") {
		t.Fatalf("got %v", err)
	}
}
