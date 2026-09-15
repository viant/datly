package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmitScaffoldUpdatesShapesAppendOnly(t *testing.T) {
	for _, conflict := range []string{"", "type", "tag"} {
		t.Run(conflict, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
			plan.Output.Fields = []Field{{Name: "Z", Type: "string", Tag: `json:"z"`}, {Name: "A", Type: "int"}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "output.go")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			before = []byte(string(before) + "\n// authored tail remains\n")
			if err = os.WriteFile(path, before, 0644); err != nil {
				t.Fatal(err)
			}
			plan.Output.Fields = []Field{{Name: "A", Type: "int"}, {Name: "B", Type: "bool"}, {Name: "Z", Type: "string", Tag: `json:"z"`}}
			if conflict == "type" {
				plan.Output.Fields[0].Type = "string"
			}
			if conflict == "tag" {
				plan.Output.Fields[2].Tag = `json:"renamed"`
			}
			plan.Description = "updated router"
			oldRouter, err := os.ReadFile(filepath.Join(dir, plan.RouterDest))
			if err != nil {
				t.Fatal(err)
			}
			_, err = EmitScaffold(dir, plan)
			after, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if conflict != "" {
				if err == nil || !strings.Contains(err.Error(), "explicit migration required") {
					t.Fatalf("error %v", err)
				}
				if string(before) != string(after) {
					t.Fatal("failed update changed shape")
				}
				router, _ := os.ReadFile(filepath.Join(dir, plan.RouterDest))
				if string(router) != string(oldRouter) {
					t.Fatal("failed update partially published router")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			text := string(after)
			z, a, b := strings.Index(text, "Z string"), strings.Index(text, "A int"), strings.Index(text, "B bool")
			if z < 0 || a < z || b < a || !strings.Contains(text, "// authored tail remains") {
				t.Fatalf("append-only shape: %s", text)
			}
			if _, err = EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			again, _ := os.ReadFile(path)
			if string(again) != text {
				t.Fatal("no-op update changed bytes")
			}
		})
	}
}

func TestEmitScaffoldRetainsObsoleteShapeFiles(t *testing.T) {
	for _, oldManifest := range []bool{false, true} {
		t.Run(fmt.Sprint(oldManifest), func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}}}}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			if oldManifest {
				manifest, err := readScaffoldManifest(dir)
				if err != nil {
					t.Fatal(err)
				}
				if err = writeScaffoldManifest(dir, manifest.Owner, manifest.Files); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(filepath.Join(dir, "records.go"))
			if err != nil {
				t.Fatal(err)
			}
			plan.Views = nil
			if _, err = EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(dir, "records.go"))
			if err != nil || string(before) != string(after) {
				t.Fatalf("obsolete shape not retained: %s, %v", after, err)
			}
			manifest, err := readScaffoldManifest(dir)
			if err != nil || manifest.Roles["records.go"] != "shape" {
				t.Fatalf("shape role %+v, %v", manifest, err)
			}
		})
	}
}

func TestEmitScaffoldRejectsAmbiguousOldManifestCleanup(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := []byte("package records\ntype Row struct{}\nfunc Helper(){}\n")
	if err = os.WriteFile(filepath.Join(dir, "prior.go"), old, 0644); err != nil {
		t.Fatal(err)
	}
	manifest.Files = append(manifest.Files, "prior.go")
	if err = writeScaffoldManifest(dir, manifest.Owner, manifest.Files); err != nil {
		t.Fatal(err)
	}
	for _, validate := range []bool{true, false} {
		if validate {
			err = plan.ValidateDestination(dir)
		} else {
			_, err = EmitScaffold(dir, plan)
		}
		if err == nil || !strings.Contains(err.Error(), "migrate its manifest role") {
			t.Fatalf("unknown ownership error %v", err)
		}
	}
	retained, err := os.ReadFile(filepath.Join(dir, "prior.go"))
	if err != nil || string(retained) != string(old) {
		t.Fatal("ambiguous prior shape changed")
	}
}
