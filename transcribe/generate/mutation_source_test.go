package generate

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func TestMutationSupportRejectsConflicts(t *testing.T) {
	for _, test := range []struct{ name, destination, source string }{
		{"parent-path", "../support.go", "const extra = 1"},
		{"nested-package", "nested/support.go", "const extra = 1"},
		{"main-destination", "orders_mutation_gen.go", "const extra = 1"},
		{"main-symbol", "support.go", "type ordersDefinition struct{}"},
		{"contract-symbol", "support.go", "type OrdersInput struct{}"},
		{"implicit-init", "support.go", "func init(){}"},
		{"sql-import", "support.go", "import _ \"github.com/viant/datly/sql/dml\""},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "support.go", "package authored\n"+test.source, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			asset := mutationAsset(t, mutationDefinitionFixture)
			asset.Support = []MutationSource{{Destination: test.destination, File: file}}
			if _, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset}).Plan(); err == nil {
				t.Fatal("invalid support accepted")
			}
		})
	}
}

func TestMutationSupportOwnsSnapshotAndProtectsEdits(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "support.go", "package authored\nconst extra = 1", 0)
	if err != nil {
		t.Fatal(err)
	}
	asset := mutationAsset(t, mutationDefinitionFixture)
	asset.Support = []MutationSource{{Destination: "support_gen.go", File: file}}
	input := Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset}
	generator := New(input)
	file.Decls = nil
	plan, err := generator.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.MutationHandler.Support[0].File.Decls) == 0 {
		t.Fatal("caller mutated generator snapshot")
	}
	plan.MutationHandler.Support[0].File.Decls = nil
	dir := t.TempDir()
	if _, err = generator.Generate(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "support_gen.go")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edited := append(body, []byte("\n// authored change\n")...)
	if err = os.WriteFile(path, edited, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = generator.Generate(dir); err == nil {
		t.Fatal("edited support overwritten")
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != string(edited) {
		t.Fatal("edited support was modified")
	}
}

func TestMutationSupportRejectsDuplicateCompanionSymbols(t *testing.T) {
	for _, declaration := range []string{"const duplicate = 1", "type duplicate struct{}", "func duplicate(){}"} {
		file, err := parser.ParseFile(token.NewFileSet(), "support.go", "package authored\n"+declaration, 0)
		if err != nil {
			t.Fatal(err)
		}
		asset := mutationAsset(t, mutationDefinitionFixture)
		asset.Support = []MutationSource{{Destination: "first_gen.go", File: file}, {Destination: "second_gen.go", File: file}}
		if _, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset}).Plan(); err == nil {
			t.Fatalf("duplicate companion declaration accepted: %s", declaration)
		}
	}
}
