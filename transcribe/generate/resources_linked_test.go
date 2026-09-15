package generate

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestLinkedPackageResourcesRetainOwnership(t *testing.T) {
	for _, name := range []string{"retain", "prefixed-symbol", "edited", "disabled", "missing", "foreign-namespace"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			pkg := "example.com/generated/records"
			dir := filepath.Join(root, "records")
			key := spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}
			resources := &ResourcePlan{Namespace: fmt.Sprintf("datly_%x", sha256.Sum256([]byte(pkg+":Records"))), Destination: "records_resources.go", Files: []EmittedFile{{Path: "query.sql", Content: "SELECT 1"}}}
			if name == "prefixed-symbol" {
				resources.Symbol = "Records"
			}
			initial := &Plan{Package: pkg, GoPackage: "records", ComponentPackage: pkg, OwnerIdentity: key.String(), ComponentName: key.Name, RouterDest: "records_router.go",
				Input: generatedContract("RecordsInput", "records_input.go"), Output: generatedContract("RecordsOutput", "records_output.go"), Resources: resources}
			if _, err := EmitScaffold(dir, initial); err != nil {
				t.Fatal(err)
			}
			before, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if name == "edited" {
				for file, content := range map[string]string{"query.sql": "SELECT 42", resources.Destination: resources.source("records") + "\n// authored resource note\n"} {
					if err = os.WriteFile(filepath.Join(dir, file), []byte(content), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if name == "missing" {
				if err = os.Remove(filepath.Join(dir, "query.sql")); err != nil {
					t.Fatal(err)
				}
			}
			if name == "foreign-namespace" {
				before.Resources.Namespace = "foreign"
				if err = writeScaffoldManifest(dir, key.Name, before.Files, before); err != nil {
					t.Fatal(err)
				}
			}
			catalog := typecatalog.NewCatalog()
			input := x.NewType(reflect.TypeOf(struct{}{}), x.WithName("RecordsInput"), x.WithPkgPath(pkg))
			output := x.NewType(reflect.TypeOf(struct{}{}), x.WithName("RecordsOutput"), x.WithPkgPath(pkg))
			if err = catalog.RegisterAll(typecatalog.TypeOriginPackage, input, output); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := New(Input{Component: &spec.Component{Key: key, Name: key.Name, Settings: &spec.Settings{Generation: &spec.GenerationSettings{ResourcesFile: resources.Destination}}}, TargetPackage: pkg, PackageName: "records", ProjectRoot: root, TypeResolver: resolver, SQLResources: name != "disabled",
				Contracts: ContractReferences{Input: &ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}}}).Plan()
			if name == "missing" || name == "foreign-namespace" {
				if err == nil {
					t.Fatal("invalid linked resources accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "disabled" {
				if err = plan.ValidateDestination(dir); err == nil || !strings.Contains(err.Error(), "before disabling resource generation") {
					t.Fatalf("disabled linked resources: %v", err)
				}
				return
			}
			if _, err = EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			after, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{resources.Destination, "query.sql"} {
				if after.Fingerprints[file] != before.Fingerprints[file] {
					t.Fatalf("retained file %s acquired a new ownership baseline", file)
				}
				content, err := os.ReadFile(filepath.Join(dir, file))
				if err != nil {
					t.Fatal(err)
				}
				want := plan.Resources.source("records")
				if file == "query.sql" {
					want = plan.Resources.Files[0].Content
				}
				if string(content) != want {
					t.Fatalf("retained file %s changed", file)
				}
			}
			if name == "edited" {
				plan.Resources.sourceText = ""
				plan.Resources.Files[0].Content = "SELECT 2"
				if err = plan.ValidateDestination(dir); err == nil || !strings.Contains(err.Error(), "manually changed") {
					t.Fatalf("retained edits lost replacement protection: %v", err)
				}
			}
		})
	}
}
