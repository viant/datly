package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

// Public authoring cannot write a nested Go resource. The separate prepared
// forest witness below deliberately records that boundary rather than claiming
// that its test-only intermediary insertion is an authored EmitScaffold action.
func TestPreparedProjectedImportPublicControls(t *testing.T) {
	t.Run("ordinary_nominal_import", func(t *testing.T) {
		root, dir, plan := preparedProjectedImportFixture(t)
		bridge := filepath.Join(dir, "bridge")
		if err := os.MkdirAll(bridge, 0750); err != nil {
			t.Fatal(err)
		}
		content := "package bridge\ntype Token struct{}\n"
		if err := os.WriteFile(filepath.Join(bridge, "bridge.go"), []byte(content), 0640); err != nil {
			t.Fatal(err)
		}
		preparedProjectedImportUseBridge(plan)
		for round := 0; round < 2; round++ {
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatalf("ordinary nominal import round %d: %v", round, err)
			}
			got, err := os.ReadFile(filepath.Join(dir, "shape.go"))
			if err != nil || !strings.Contains(string(got), `"example.com/projectedimports/api/bridge"`) || strings.Contains(string(got), "-stage-") {
				t.Fatalf("generated import must retain nominal identity: %s, %v", got, err)
			}
			got, err = os.ReadFile(filepath.Join(bridge, "bridge.go"))
			if err != nil || string(got) != content {
				t.Fatalf("unmanaged intermediary changed: %q, %v", got, err)
			}
			info, err := os.Stat(filepath.Join(bridge, "bridge.go"))
			if err != nil || info.Mode().Perm() != 0640 {
				t.Fatalf("unmanaged intermediary mode changed: %v", err)
			}
		}
		preparedProjectedImportNoStages(t, root)
	})
	t.Run("nested_go_resource_rejected_before_preparation", func(t *testing.T) {
		root, dir, plan := preparedProjectedImportFixture(t)
		before, err := readScaffoldSnapshot(root)
		if err != nil {
			t.Fatal(err)
		}
		plan.Resources = &ResourcePlan{Namespace: "sql", Destination: "resources.go", Files: []EmittedFile{{Path: "bridge/bridge.go", Content: "package bridge\ntype Token struct{}\n"}}}
		_, err = EmitScaffold(dir, plan)
		if err == nil || !strings.Contains(err.Error(), "bridge/bridge.go") {
			t.Fatalf("expected package-local Go destination rejection: %v", err)
		}
		t.Logf("public nested-Go preflight boundary: %v", err)
		preparedProjectedImportUnchanged(t, root, before)
	})
}

// Scope: genuine packageSet render, validation and forest preparation followed
// by a test-only unmanaged Go package insertion in that private forest. The
// insertion is not a supported public generated resource destination.
func TestPreparedProjectedImportNewIntermediaryAfterPreparation(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		name := "acyclic_nominal_control"
		if cycle {
			name = "new_intermediary_cycle_rejected"
		}
		t.Run(name, func(t *testing.T) {
			root, dir, plan := preparedProjectedImportFixture(t)
			before, err := readScaffoldSnapshot(root)
			if err != nil {
				t.Fatal(err)
			}
			preparedProjectedImportUseBridge(plan)
			packages, err := plan.packageTargets(dir)
			if err != nil {
				t.Fatal(err)
			}
			release, err := packages.lockTargets()
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if err = packages.render(); err != nil {
				t.Fatal(err)
			}
			if err = packages.validate(); err != nil {
				t.Fatal("ordinary pre-preparation validation:", err)
			}
			forests, readRoots, _, err := prepareScaffoldForests(packages.templates)
			if err != nil {
				t.Fatal("genuine forest preparation:", err)
			}
			defer cleanupScaffoldForests(forests)
			packages.forests, packages.readRoots = forests, readRoots
			if len(forests) != 1 || len(readRoots) != 2 || forests[0].target != dir {
				t.Fatalf("expected genuine main/shape projected forest: %#v, %v", forests, readRoots)
			}
			if err = packages.validateProjected(); err != nil {
				t.Fatal("prepared tree before new intermediary:", err)
			}
			bridge := filepath.Join(readRoots[0], "bridge")
			if _, err = os.Stat(filepath.Join(dir, "bridge")); !os.IsNotExist(err) {
				t.Fatalf("intermediary unexpectedly existed in original tree: %v", err)
			}
			if err = os.MkdirAll(bridge, 0750); err != nil {
				t.Fatal(err)
			}
			content := "package bridge\ntype Token struct{}\n"
			if cycle {
				content = "package bridge\nimport api \"example.com/projectedimports/api\"\ntype Token = api.MainRow\n"
			}
			if err = os.WriteFile(filepath.Join(bridge, "bridge.go"), []byte(content), 0640); err != nil {
				t.Fatal(err)
			}
			err = packages.validateProjected()
			if !cycle && err != nil {
				t.Fatalf("new acyclic package must use nominal import identity: %v", err)
			}
			if cycle {
				if err == nil || !strings.Contains(err.Error(), "generation import cycle:") || !strings.Contains(err.Error(), "example.com/projectedimports/api/bridge") || !strings.Contains(err.Error(), "example.com/projectedimports/api ->") || strings.Contains(err.Error(), "-stage-") {
					t.Fatalf("new projected intermediary cycle must reject with nominal paths: %v", err)
				}
				t.Logf("actual final projected-tree rejection: %v", err)
			}
			// No publication is attempted by this direct preparation witness.
			cleanupScaffoldForests(forests)
			preparedProjectedImportUnchanged(t, root, before)
		})
	}
}

func preparedProjectedImportFixture(t *testing.T) (string, string, *Plan) {
	t.Helper()
	root := t.TempDir()
	const module = "example.com/projectedimports"
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+module+"\n\ngo 1.25.8\n"), 0644); err != nil {
		t.Fatal(err)
	}
	shape := &Plan{ProjectRoot: root, Package: module + "/api/models", GoPackage: "models", ComponentName: "Shape", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", Views: []ViewPlan{{Name: "ShapeRow", Type: "ShapeRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}}}
	plan := &Plan{ProjectRoot: root, Package: module + "/api", GoPackage: "api", ComponentName: "Main", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", ShapePackages: []*Plan{shape}, Views: []ViewPlan{{Name: "MainRow", Type: "MainRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}}}
	dir := filepath.Join(root, "api")
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal("initial genuine ordinary forest:", err)
	}
	return root, dir, plan
}

func preparedProjectedImportUseBridge(plan *Plan) {
	plan.Views[0].Fields = []Field{{Name: "Bridge", Type: "bridge.Token"}}
	plan.Imports = []spec.ImportSpec{{Alias: "bridge", Package: "example.com/projectedimports/api/bridge"}}
}

func preparedProjectedImportUnchanged(t *testing.T, root string, before scaffoldSnapshot) {
	t.Helper()
	after, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("private preparation/validation changed original full paths, modes or contents")
	}
	preparedProjectedImportNoStages(t, root)
}

func preparedProjectedImportNoStages(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(entry.Name(), "-stage-") || strings.Contains(entry.Name(), "-backup-") {
			t.Errorf("leaked temporary artifact: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
