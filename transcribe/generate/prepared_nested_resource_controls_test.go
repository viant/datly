package generate

import (
	"os"
	"path/filepath"
	"testing"
)

// This captures existing EmitScaffold resource order, including a later
// parent's retirement of a shared embedded file. It is not Go-build or
// resource-complete artifact acceptance.
func TestPreparedNestedResourceOrdinaryOrderControls(t *testing.T) {
	for _, order := range []string{"parent_before_child", "child_before_parent"} {
		t.Run(order, func(t *testing.T) {
			root := t.TempDir()
			const module = "example.com/nestedresources"
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+module+"\n\ngo 1.25.8\n"), 0644); err != nil {
				t.Fatal(err)
			}
			parentDir, childDir := filepath.Join(root, "api"), filepath.Join(root, "api", "models")
			shape := &Plan{
				ProjectRoot: root, ComponentName: "Shape", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go",
				Views: []ViewPlan{{Name: "ShapeRow", Type: "ShapeRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
			}
			main := &Plan{
				ProjectRoot: root, ComponentName: "Main", RouterDest: "router.go",
				Input: generatedContract("MainInput", "input.go"), Output: generatedContract("MainOutput", "output.go"),
				ShapePackages: []*Plan{shape},
			}
			mainDir, parent, child := parentDir, main, shape
			if order == "child_before_parent" {
				mainDir, parent, child = childDir, shape, main
			}
			parent.Package, parent.GoPackage = module+"/api", "api"
			child.Package, child.GoPackage = module+"/api/models", "models"
			parent.Resources = &ResourcePlan{
				Namespace: "parent_sql", Destination: "parent_resources.go",
				Files: []EmittedFile{{Path: "models/sql/shared.sql", Content: "SELECT 1 AS id -- parent initial\n"}, {Path: "models/sql/retired.sql", Content: "SELECT 1 AS id -- parent retired\n"}},
			}
			child.Resources = &ResourcePlan{
				Namespace: "child_sql", Destination: "child_resources.go",
				Files: []EmittedFile{{Path: "sql/shared.sql", Content: "SELECT 1 AS id -- child initial\n"}, {Path: "sql/child.sql", Content: "SELECT 1 AS id -- child retained\n"}},
			}
			checkBytes := func(name, want string) {
				t.Helper()
				got, err := os.ReadFile(name)
				if err != nil || string(got) != want {
					t.Fatalf("resource %s = %q, error=%v, want %q", name, got, err, want)
				}
			}
			checkRootModes := func(phase string) {
				t.Helper()
				for _, dir := range []string{parentDir, childDir} {
					info, err := os.Stat(dir)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != 0700 {
						t.Fatalf("%s target root %s mode=%o, want existing MkdirTemp/swap mode0700", phase, dir, info.Mode().Perm())
					}
					t.Logf("%s logical target root %s mode=%o", phase, filepath.Base(dir), info.Mode().Perm())
				}
			}
			shared := filepath.Join(childDir, "sql", "shared.sql")
			if _, err := EmitScaffold(mainDir, main); err != nil {
				t.Fatal("initial generation:", err)
			}
			initialWant := "SELECT 1 AS id -- child initial\n"
			if order == "child_before_parent" {
				initialWant = "SELECT 1 AS id -- parent initial\n"
			}
			checkBytes(shared, initialWant)
			checkRootModes("initial")
			// The native per-target swap replaces the root directory mode. This
			// deliberately records that behavior rather than introducing a fix.
			if err := os.Chmod(parentDir, 0751); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(childDir, 0750); err != nil {
				t.Fatal(err)
			}
			parent.Resources.Files[0].Content = "SELECT 1 AS id -- parent updated\n"
			child.Resources.Files[0].Content = "SELECT 1 AS id -- child updated\n"
			if _, err := EmitScaffold(mainDir, main); err != nil {
				t.Fatal("overlapping resource update:", err)
			}
			updateWant := "SELECT 1 AS id -- child updated\n"
			if order == "child_before_parent" {
				updateWant = "SELECT 1 AS id -- parent updated\n"
			}
			checkBytes(shared, updateWant)
			checkRootModes("update")
			// Parent's old resource declaration owns shared.sql and retired.sql
			// relative to api. Its normal retirement below models is visible in
			// original authored order; no owner is reassigned to the child.
			parent.Resources.Files = []EmittedFile{{Path: "models/sql/new.sql", Content: "SELECT 1 AS id -- parent new\n"}}
			child.Resources.Files[0].Content = "SELECT 1 AS id -- child after parent removal\n"
			if _, err := EmitScaffold(mainDir, main); err != nil {
				t.Fatal("parent resource removal beneath child:", err)
			}
			if order == "parent_before_child" {
				checkBytes(shared, "SELECT 1 AS id -- child after parent removal\n")
				t.Log("parent retirement happens first; later child restores shared.sql")
			} else {
				if _, err := os.Stat(shared); !os.IsNotExist(err) {
					t.Fatalf("later parent retirement must remove earlier child's shared.sql in existing EmitScaffold order: %v", err)
				}
				t.Log("later parent retirement removes shared.sql despite retained child embed declaration; final Go/resource closure is not claimed")
			}
			if _, err := os.Stat(filepath.Join(childDir, "sql", "retired.sql")); !os.IsNotExist(err) {
				t.Fatalf("retired parent-only resource persisted: %v", err)
			}
			checkBytes(filepath.Join(childDir, "sql", "new.sql"), "SELECT 1 AS id -- parent new\n")
			checkBytes(filepath.Join(childDir, "sql", "child.sql"), "SELECT 1 AS id -- child retained\n")
			checkRootModes("removal")
		})
	}
}
