package generate

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

// These are ordinary baseline controls. Neither authored order is an error:
// targets may be ancestors of other targets, including on first generation.
func TestPreparedNestedTargetOrdinaryControls(t *testing.T) {
	for _, order := range []string{"main_parent_shape_child", "main_child_shape_parent"} {
		t.Run(order, func(t *testing.T) {
			root := t.TempDir()
			const module = "example.com/nestedstages"
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+module+"\n\ngo 1.25.8\n"), 0644); err != nil {
				t.Fatal(err)
			}
			parent, child := filepath.Join(root, "api"), filepath.Join(root, "api", "models")
			mainDir, shapeDir := parent, child
			mainPackage, shapePackage := "api", "api/models"
			mainName, shapeName := "api", "models"
			if order == "main_child_shape_parent" {
				mainDir, shapeDir = child, parent
				mainPackage, shapePackage = "api/models", "api"
				mainName, shapeName = "models", "api"
			}
			if _, err := os.Stat(parent); !os.IsNotExist(err) {
				t.Fatalf("fixture parent must be missing before initial generation: %v", err)
			}
			shape := &Plan{
				ProjectRoot: root, Package: module + "/" + shapePackage, GoPackage: shapeName,
				ComponentName: "NestedShape", ShapesOnly: true, ViewDest: "shape.go", RouterDest: "router.go",
				Views: []ViewPlan{{Name: "ShapeRow", Type: "ShapeRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
				Resources: &ResourcePlan{
					Namespace: "nested_shape_sql", Destination: "shape_resources.go",
					Files: []EmittedFile{{Path: "sql/shape.sql", Content: "SELECT 2 AS id\n"}},
				},
			}
			main := &Plan{
				ProjectRoot: root, Package: module + "/" + mainPackage, GoPackage: mainName,
				ComponentName: "NestedMain", RouterDest: "router.go",
				Input:         generatedContract("NestedInput", "input.go", Field{Name: "Id", Type: "int"}),
				Output:        generatedContract("NestedOutput", "output.go", Field{Name: "Rows", Type: "[]*shapes.ShapeRow"}),
				Imports:       []spec.ImportSpec{{Alias: "shapes", Package: module + "/" + shapePackage}},
				ShapePackages: []*Plan{shape},
				Resources: &ResourcePlan{
					Namespace: "nested_main_sql", Destination: "main_resources.go",
					Files: []EmittedFile{{Path: "sql/main.sql", Content: "SELECT 1 AS id\n"}},
				},
			}
			if _, err := EmitScaffold(mainDir, main); err != nil {
				t.Fatal("initial missing-parent ordinary generation:", err)
			}
			for path, want := range map[string]string{
				filepath.Join(mainDir, "sql", "main.sql"):   "SELECT 1 AS id\n",
				filepath.Join(shapeDir, "sql", "shape.sql"): "SELECT 2 AS id\n",
			} {
				got, err := os.ReadFile(path)
				if err != nil || string(got) != want {
					t.Fatalf("initial resource %s: %q, %v", path, got, err)
				}
			}
			for path, content := range map[string]string{
				filepath.Join(mainDir, "application.go"):  "package " + mainName + "\nconst MainApplication = true\n",
				filepath.Join(shapeDir, "application.go"): "package " + shapeName + "\nconst ShapeApplication = true\n",
			} {
				if err := os.WriteFile(path, []byte(content), 0640); err != nil {
					t.Fatal(err)
				}
			}
			// These unmanaged resource files exercise byte and mode preservation
			// independently of generated resources, which retain ordinary 0644.
			for _, dir := range []string{mainDir, shapeDir} {
				if err := os.WriteFile(filepath.Join(dir, "sql", "retained.txt"), []byte("retained user resource\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := readScaffoldSnapshot(root)
			if err != nil {
				t.Fatal(err)
			}
			for round := 1; round <= 2; round++ {
				if _, err := EmitScaffold(mainDir, main); err != nil {
					t.Fatalf("repeat generation round %d: %v", round, err)
				}
				after, err := readScaffoldSnapshot(root)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("repeat %d changed whole paths/modes/content: %v", round, err)
				}
				for _, dir := range []string{mainDir, shapeDir} {
					for name, mode := range map[string]os.FileMode{"application.go": 0640, filepath.Join("sql", "retained.txt"): 0600} {
						info, err := os.Stat(filepath.Join(dir, name))
						if err != nil || info.Mode().Perm() != mode {
							t.Fatalf("retained file %s mode: info=%v error=%v, want %o", filepath.Join(dir, name), info, err, mode)
						}
					}
				}
				if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					name := entry.Name()
					if name == legacyManifestName {
						t.Errorf("legacy generation sidecar persisted: %s", path)
					}
					for _, target := range []string{"api", "models"} {
						for _, suffix := range []string{"-stage-", "-backup-"} {
							if strings.HasPrefix(name, "."+target+suffix) {
								t.Errorf("temporary artifact persisted inside authored tree: %s", path)
							}
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			t.Log("ordinary initial missing-parent generation and two repeats preserve both nested packages, nominal shape import, all resource/user bytes and modes; no temporary artifacts")
		})
	}
}
