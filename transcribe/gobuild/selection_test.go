package gobuild

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGoFilesRetainsValidationBoundaries(t *testing.T) {
	root := t.TempDir()
	write := func(name, src string) {
		t.Helper()
		file := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/selection\ngo 1.25.0\n")
	write("endpoint/value.go", "package endpoint\nvar Value=Missing\n")
	write("endpoint/excluded_windows.go", "package endpoint\nnot valid Go\n")
	build := &Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "GOOS=linux", "CGO_ENABLED=0"}}
	selected, err := build.GoFiles("example.com/selection/endpoint", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, []string{"value.go"}) {
		t.Fatalf("selection=%v", selected)
	}
	if _, err = build.ValidateFiles("example.com/selection/endpoint", nil); err == nil || !strings.Contains(err.Error(), "Missing") {
		t.Fatalf("type error bypassed: %v", err)
	}
	staged := map[string][]byte{"endpoint/value.go": []byte("package endpoint\nconst Value=42\nfunc init(){panic(\"build must not execute initialization\")}\n")}
	if _, err = build.ValidateFiles("example.com/selection/endpoint", staged); err != nil {
		t.Fatal(err)
	}
	if _, err = build.GoFiles("example.com/selection/endpoint", map[string][]byte{filepath.Join(root, "other/value.go"): []byte("package endpoint\n")}); err == nil || !strings.Contains(err.Error(), "outside selected build package") {
		t.Fatalf("destination identity bypassed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = build.WithContext(ctx).GoFiles("example.com/selection/endpoint", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("selection cancellation=%v", err)
	}
	if _, err = build.WithContext(ctx).ValidateFiles("example.com/selection/endpoint", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("validation cancellation=%v", err)
	}
	original, err := os.ReadFile(filepath.Join(root, "endpoint/value.go"))
	if err != nil || !strings.Contains(string(original), "Missing") {
		t.Fatalf("source changed: %v", err)
	}
}

func TestGoFilesMergesProjectedAndInheritedOverlay(t *testing.T) {
	root := t.TempDir()
	write := func(name, src string) {
		t.Helper()
		file := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/selection\ngo 1.25.0\n")
	write("dependency/value.go", "package dependency\nconst Old=1\n")
	write("backing/value.go", "package dependency\nconst Value=42\n")
	write("endpoint/value.go", "package endpoint\nvar Value=Missing\n")
	write("endpoint/deleted.go", "package endpoint\nnot valid Go\n")
	write("endpoint/tagged.go", "//go:build selected\n\npackage endpoint\nconst Tagged=1\n")
	write("backing/wrong.go", "package endpoint\nvar Value=Missing\n")
	write("backing/deleted.go", "package endpoint\nnot valid Go\n")
	data, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{"dependency/value.go": "backing/value.go", "endpoint/value.go": "backing/wrong.go", "endpoint/deleted.go": "backing/deleted.go"}})
	if err != nil {
		t.Fatal(err)
	}
	write("configuration/overlay with spaces.json", string(data))
	build := &Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=-tags=selected '-overlay=configuration/overlay with spaces.json'", "CGO_ENABLED=0"}}
	staged := map[string][]byte{"endpoint/value.go": []byte("package endpoint\nimport \"example.com/selection/dependency\"\nconst Value=dependency.Value\n"), "endpoint/deleted.go": nil}
	selected, err := build.GoFiles("example.com/selection/endpoint", staged)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := build.ValidateFiles("example.com/selection/endpoint", staged)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, []string{"tagged.go", "value.go"}) || !reflect.DeepEqual(selected, validated) {
		t.Fatalf("selected/validated=%v/%v", selected, validated)
	}
	for _, name := range []string{"endpoint/value.go", "endpoint/deleted.go"} {
		current, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !strings.Contains(string(current), "Missing") && !strings.Contains(string(current), "not valid") {
			t.Fatalf("staged source changed: %s: %v", name, err)
		}
	}
}
