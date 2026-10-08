package generate

import (
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLifecycleReachabilitySharedLocalPackage(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/reachability"}).Write(t, root)
	dir := filepath.Join(root, "generated")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "hooks.go"), []byte("package generated\ntype Input struct{}\ntype Output struct{}\ntype SharedLifecycle struct{}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Orders", "Invoices"} {
		plan := &Plan{Package: "example.com/reachability/generated", ComponentName: name, Holder: name + "Component", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), LifecycleTypes: []string{"SharedLifecycle", "same.SharedLifecycle", "SharedLifecycle"}, Imports: []spec.ImportSpec{{Alias: "same", Package: "example.com/reachability/generated"}}}
		source, err := componentFileText("generated", plan)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(source, ".TypeFor[SharedLifecycle]()") != 1 {
			t.Fatalf("equivalent local hook declarations not deduplicated: %s", source)
		}
		if strings.Contains(source, `"example.com/reachability/generated"`) {
			t.Fatal("local alias introduced self import")
		}
		if err = os.WriteFile(filepath.Join(dir, strings.ToLower(name)+"_component.go"), []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "-race", "./generated")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("shared local components must compile without duplicate symbols: %v\n%s", err, output)
	}
}

func TestLifecycleReachabilityPreservesExternalAndHooklessEmission(t *testing.T) {
	plan := &Plan{Package: "example.com/reachability/generated", ComponentName: "Orders", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
	before, err := componentFileText("generated", plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(before, "_anchorSharedLifecycle_") {
		t.Fatal("hookless emission changed")
	}
	plan.Imports = []spec.ImportSpec{{Alias: "foreign", Package: "example.com/reachability/hooks"}}
	plan.LifecycleTypes = []string{"foreign.Rules"}
	foreign, err := componentFileText("generated", plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(foreign, "var _anchorForeignRules = reflect.TypeFor[foreign.Rules]()") || strings.Contains(foreign, "_anchorSharedLifecycle_") {
		t.Fatalf("foreign reachability changed: %s", foreign)
	}
	for _, expression := range []string{"unknown.Rules", "func()", "a.b.c", "[]Rules", "type"} {
		plan.LifecycleTypes = []string{expression}
		if len(localLifecycleAnchors(plan)) != 0 {
			t.Fatalf("invalid/unowned expression admitted as local: %s", expression)
		}
	}
}
