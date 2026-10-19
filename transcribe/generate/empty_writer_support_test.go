package generate

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/spec"
)

func TestEmptyWriterSupportIsNotEmittedAndLegacyFilesRetire(t *testing.T) {
	for _, custom := range []bool{false, true} {
		name := "default"
		if custom {
			name = "custom"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
			plan.Settings.Mutation = "patch"
			if custom {
				plan.Generation = &spec.GenerationSettings{FilePrefix: "records_", MutationFile: "write.go", LinksFile: "relations.go", SupportFiles: map[string]string{"previous": "old_rows.go"}}
			}
			files, err := EmitScaffold(dir, plan)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if filepath.Ext(file.Path) != ".go" {
					continue
				}
				parsed, err := parser.ParseFile(token.NewFileSet(), file.Path, file.Content, parser.ParseComments)
				if err != nil {
					t.Fatal(err)
				}
				if len(parsed.Decls) == 0 {
					t.Fatalf("empty generated Go artifact: %s", file.Path)
				}
			}
			roles := []string{"mutation", "links", "frames", "previous", "layout", "actions", "mutation_output", "validation"}
			for _, role := range roles {
				path := filepath.Join(dir, plan.Generation.File(role, role+".go"))
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("empty role emitted: %s (%v)", path, err)
				}
				legacy := generatedHeader("Records") + "package records\n\n// Generated " + role + " support is executed by Datly's universal mutation writer.\n"
				if err := os.WriteFile(path, []byte(legacy), 0644); err != nil {
					t.Fatal(err)
				}
			}
			authored := filepath.Join(dir, "application.go")
			const source = "package records\nconst ApplicationOwned = true\n"
			if err := os.WriteFile(authored, []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			for _, role := range roles {
				path := filepath.Join(dir, plan.Generation.File(role, role+".go"))
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("obsolete empty role retained: %s (%v)", path, err)
				}
			}
			actual, err := os.ReadFile(authored)
			if err != nil || string(actual) != source {
				t.Fatalf("authored code changed: %v", err)
			}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal("repeated generation", err)
			}
		})
	}
}
