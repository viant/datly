package build

import (
	"context"
	xmodule "github.com/viant/x/module"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLinkerRejectsUnlinkableDeclarations(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"private shape", "type hidden struct{};type Input struct{Value *hidden};type Output struct{}", "must be exported"},
		{"factory arguments", "type Input struct{};type Output struct{};func NewWrite(x int)int{return x}", "zero-argument"},
		{"unsupported factory", "type Input struct{};type Output struct{};func NewWrite()int{return 1}", "return Contract"},
		{"generic declaration", "type Input[T any] struct{Value T};type Output struct{}", "uninstantiated generic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/app\ngo 1.25.8\n"), 0644); err != nil {
				t.Fatal(err)
			}
			handler := ""
			if strings.Contains(tc.name, "factory") {
				handler = ",handler=NewWrite"
			}
			source := "package app\nimport xdatly \"github.com/viant/xdatly\"\ntype Components struct{C xdatly.Component[Input,Output] `component:\"C,path=/c,method=GET" + handler + "\"`}\n" + tc.body
			if err := os.WriteFile(filepath.Join(root, "app.go"), []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			selection := &xmodule.BuildSelection{Packages: []xmodule.BuildPackage{{Dir: root, ImportPath: "example.com/app", Name: "app", GoFiles: []string{"app.go"}, Module: &xmodule.BuildModule{Path: "example.com/app", Dir: root, Main: true}}}}
			_, _, err := newLinker(selection).generate(context.Background(), "example.com/app")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
		})
	}
}
func TestBuildPreservesEditedLinkerAndLock(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/app\ngo 1.25.8\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := (Service{}).Init(context.Background(), InitRequest{Dir: root}); err != nil {
		t.Fatal(err)
	}
	m := managedFiles{root: root}
	data := []byte("// authored edit\n" + linkStub)
	if err := m.write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := (Service{}).Build(context.Background(), Request{Dir: root}); err == nil || !strings.Contains(err.Error(), "was edited") {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, linkPath))
	if err != nil || string(actual) != string(data) {
		t.Fatal("edited linker changed")
	}
	if err = m.restore([]byte(linkStub)); err != nil {
		t.Fatal(err)
	}
	release, err := m.lock()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err = (Service{}).Build(context.Background(), Request{Dir: root}); err == nil || !strings.Contains(err.Error(), "build lock") {
		t.Fatal(err)
	}
}
