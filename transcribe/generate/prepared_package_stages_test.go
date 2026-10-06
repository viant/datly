package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This regression uses ordinary generated packages and a real staged write
// failure. It makes no identity, compiler ownership or publication assertion.
func TestPreparedPackageStagesSecondWriteFailureLeavesAllTargets(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/preparedstages\n\ngo 1.25.8\n"), 0644); err != nil {
		t.Fatal(err)
	}
	firstShape := &Plan{
		ProjectRoot: root, Package: "example.com/preparedstages/firstshape", GoPackage: "firstshape",
		ComponentName: "FirstShape", ShapesOnly: true, ViewDest: "shape.go", RouterDest: "router.go",
		Views: []ViewPlan{{Name: "FirstShape", Type: "FirstShape", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
	}
	secondShape := &Plan{
		ProjectRoot: root, Package: "example.com/preparedstages/secondshape", GoPackage: "secondshape",
		ComponentName: "SecondShape", ShapesOnly: true, ViewDest: "shape.go", RouterDest: "router.go",
		Views: []ViewPlan{{Name: "SecondShape", Type: "SecondShape", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
	}
	main := &Plan{
		ProjectRoot: root, Package: "example.com/preparedstages/mainpkg", GoPackage: "mainpkg",
		ComponentName: "Main", RouterDest: "router.go",
		Input:         generatedContract("MainInput", "input.go", Field{Name: "Before", Type: "bool"}),
		Output:        generatedContract("MainOutput", "output.go"),
		ShapePackages: []*Plan{firstShape, secondShape},
	}
	directories := []string{filepath.Join(root, "mainpkg"), filepath.Join(root, "firstshape"), filepath.Join(root, "secondshape")}
	if _, err := EmitScaffold(directories[0], main); err != nil {
		t.Fatal("initial ordinary three-target generation:", err)
	}
	for _, dir := range directories {
		if err := os.WriteFile(filepath.Join(dir, "retained.txt"), []byte("unmanaged content retained\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// A regular file is copied into the second target's stage. The supported
	// nested SQL resource passes preview, then MkdirAll must fail at write.
	// Go shape destinations remain package-local as required by validation.
	blocked := filepath.Join(directories[1], "blocked")
	if err := os.WriteFile(blocked, []byte("regular file, not a directory\n"), 0640); err != nil {
		t.Fatal(err)
	}
	before, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	main.Input.Fields = []Field{{Name: "After", Type: "bool"}}
	firstShape.Resources = &ResourcePlan{
		Namespace: "first_shape_sql", Destination: "resources.go",
		Files: []EmittedFile{{Path: "blocked/shape.sql", Content: "SELECT 1 AS id\n"}},
	}
	secondShape.Views[0].Fields = []Field{{Name: "After", Type: "bool"}}
	// ValidateDestination is the genuine public preflight. If it fails here,
	// this fixture did not reach the intended stage-writing checkpoint.
	if err := main.ValidateDestination(directories[0]); err != nil {
		t.Fatal("fixture failed before stage writing:", err)
	}
	t.Log("ordinary preview passed for main plus two ShapePackages; injecting real blocked/shape.sql staged write failure")
	_, err = EmitScaffold(directories[0], main)
	if err == nil {
		t.Fatal("expected second-target staged write failure")
	}
	t.Logf("actual staged write error: %v", err)
	if !strings.Contains(err.Error(), "blocked") || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("failure was not the intended blocked parent MkdirAll: %v", err)
	}
	after, snapshotErr := readScaffoldSnapshot(root)
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if !reflect.DeepEqual(before, after) {
		var changed []string
		for name, value := range before {
			if current, ok := after[name]; !ok || current != value {
				changed = append(changed, name)
			}
		}
		for name := range after {
			if _, ok := before[name]; !ok {
				changed = append(changed, name)
			}
		}
		sort.Strings(changed)
		t.Errorf("preparing the second target changed original whole paths/modes/content: %v", changed)
	}
	for _, dir := range directories {
		entries, err := os.ReadDir(filepath.Dir(dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			for _, suffix := range []string{"-stage-", "-backup-"} {
				if strings.HasPrefix(entry.Name(), "."+filepath.Base(dir)+suffix) {
					t.Errorf("leaked owned preparation artifact %s", filepath.Join(filepath.Dir(dir), entry.Name()))
				}
			}
		}
	}
}
