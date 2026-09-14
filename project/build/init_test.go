package build_test

import (
	"bytes"
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/project/build"
	"golang.org/x/mod/modfile"
	"os"
	"path/filepath"
	"testing"
)

func TestInitNewModulePreservesAuthoredChoices(t *testing.T) {
	root := t.TempDir()
	source, err := (testharness.GeneratedModule{}).DependencyDir("github.com/viant/datly")
	if err != nil {
		t.Fatal(err)
	}
	request := build.InitRequest{Dir: root, Module: "example.com/newapp", Local: map[string]string{"github.com/viant/datly": source}, Pins: map[string]string{"example.com/pinned": "v1.2.3"}}
	svc := build.Service{}
	if err = svc.Init(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := modfile.Parse("go.mod", manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Replace) != 1 || len(parsed.Require) != 2 {
		t.Fatal(string(manifest))
	}
	modified := append([]byte("// authored module comment\n"), manifest...)
	if err = os.WriteFile(filepath.Join(root, "go.mod"), modified, 0644); err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(root, "cmd/datly/main.go")
	authored := []byte("package main\n// keep authored command\nfunc main(){}\n")
	if err = os.WriteFile(main, authored, 0644); err != nil {
		t.Fatal(err)
	}
	if err = svc.Init(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(filepath.Join(root, "go.mod"))
	if !bytes.Equal(actual, modified) {
		t.Fatal("module bytes changed")
	}
	actual, _ = os.ReadFile(main)
	if !bytes.Equal(actual, authored) {
		t.Fatal("command edits lost")
	}
	request.Pins["example.com/pinned"] = "v1.2.4"
	if err = svc.Init(context.Background(), request); err != nil {
		t.Fatalf("initialized project must be a no-op: %v", err)
	}
	actual, _ = os.ReadFile(filepath.Join(root, "go.mod"))
	if !bytes.Equal(actual, modified) {
		t.Fatal("initialized project dependency changed")
	}
}
func TestInitRejectsUnpinnedNewModule(t *testing.T) {
	for _, pin := range []string{"", "latest", "v1", "v1.2"} {
		t.Run(pin, func(t *testing.T) {
			r := build.InitRequest{Dir: t.TempDir(), Module: "example.com/newapp"}
			if pin != "" {
				r.Pins = map[string]string{"github.com/viant/datly": pin}
			}
			if err := (build.Service{}).Init(context.Background(), r); err == nil {
				t.Fatal("accepted unpinned initialization")
			}
		})
	}
}

func TestInitExistingDependencyPackageIsNoop(t *testing.T) {
	for _, dependency := range []string{"github.com/viant/datly"} {
		t.Run(dependency, func(t *testing.T) {
			root := t.TempDir()
			module := []byte("module example.com/existing\n\ngo 1.25.8\nrequire " + dependency + " v0.0.0\n")
			if err := os.WriteFile(filepath.Join(root, "go.mod"), module, 0644); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(root, "pkg/dependency")
			if err := os.MkdirAll(directory, 0755); err != nil {
				t.Fatal(err)
			}
			content := []byte("package dependency\n// Authored package; leave unchanged.\n")
			if err := os.WriteFile(filepath.Join(directory, "dependencies.go"), content, 0644); err != nil {
				t.Fatal(err)
			}
			request := build.InitRequest{Dir: root, Module: "example.com/different", Pins: map[string]string{dependency: "latest"}}
			if err := (build.Service{}).Init(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			actual, _ := os.ReadFile(filepath.Join(root, "go.mod"))
			if !bytes.Equal(actual, module) {
				t.Fatal("go.mod changed")
			}
			actual, _ = os.ReadFile(filepath.Join(directory, "dependencies.go"))
			if !bytes.Equal(actual, content) {
				t.Fatal("dependency package changed")
			}
			if _, err := os.Stat(filepath.Join(root, "datly.yaml")); !os.IsNotExist(err) {
				t.Fatalf("unexpected scaffolding: %v", err)
			}
		})
	}
}

func TestInitRejectsInvalidExistingModule(t *testing.T) {
	root := t.TempDir()
	content := []byte("module \"example.com/invalid path\"\nrequire github.com/viant/datly v0.0.0\n")
	if err := os.WriteFile(filepath.Join(root, "go.mod"), content, 0644); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "pkg/dependency")
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "init.go"), []byte("package dependency\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := (build.Service{}).Init(context.Background(), build.InitRequest{Dir: root}); err == nil {
		t.Fatal("invalid existing module accepted")
	}
	actual, _ := os.ReadFile(filepath.Join(root, "go.mod"))
	if !bytes.Equal(content, actual) {
		t.Fatal("invalid module modified")
	}
}
