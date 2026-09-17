package generate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPackageSetSkipsModuleRootWithoutGoSource(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/datly-studio\n\ngo 1.25.8\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "api", "records")
	hyphenated := filepath.Join(root, "cmd", "studio-migrate")
	if err := os.MkdirAll(hyphenated, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hyphenated, "main.go"), []byte("package main\nfunc main(){}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	set := &packageSet{
		plans: []*Plan{{ProjectRoot: root, Package: "example.com/datly-studio/api/records", ComponentName: "Records"}},
		dirs:  []string{directory},
		files: [][]EmittedFile{{{Path: filepath.Join(directory, "views.go"), Content: "package records\ntype Record struct{}\n"}}},
	}
	if err := set.validateImports(); err != nil {
		t.Fatalf("hyphenated module root without Go source blocked generation: %v", err)
	}
}
