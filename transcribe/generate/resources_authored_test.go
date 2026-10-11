package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestLinkedAuthoredResourcesSurviveGeneratedEnvelopes(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "retain", true: "reject changed declaration"}[stale], func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			pkg := "example.com/generated/records"
			dir := filepath.Join(root, "records")
			if err := os.MkdirAll(filepath.Join(dir, "sql"), 0755); err != nil {
				t.Fatal(err)
			}
			source := `package records
import "embed"
const RecordsDatlyResourceNamespace = "` + readableResourceNamespace(pkg, "Records") + `"
// Application-owned resource declaration and capability alias.
//go:embed "sql/records.sql" "sql/original-child.sql"
var RecordsDatlyResources embed.FS
var OriginalFS = RecordsDatlyResources
`
			for file, content := range map[string]string{"row.go": "package records\ntype Row struct { ID int }\n", "resources.go": source, "sql/records.sql": "SELECT ID FROM records", "sql/original-child.sql": "SELECT ID FROM original_child"} {
				if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			catalog := typecatalog.NewCatalog()
			row := x.NewType(reflect.TypeOf(struct{ ID int }{}), x.WithName("Row"), x.WithPkgPath(pkg))
			if err := catalog.Register(typecatalog.TypeOriginPackage, row); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}, Name: "Records", Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT ID FROM records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}}}}}
			plan, err := New(Input{Component: component, TargetPackage: pkg, PackageName: "records", ProjectRoot: root, TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			if plan.Input.Ownership != ContractGenerated || plan.Output.Ownership != ContractGenerated || !plan.Resources.authoredSource() {
				t.Fatal("lost authored resource authority")
			}
			want := source
			if stale {
				want = source + "\n// concurrent application edit\n"
				if err = os.WriteFile(filepath.Join(dir, "resources.go"), []byte(want), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = EmitScaffold(dir, plan)
			if stale && err == nil {
				t.Fatal("changed application resource was accepted")
			}
			if !stale && err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "resources.go"))
			if err != nil || string(data) != want {
				t.Fatalf("application declaration replaced: %v\n%s", err, data)
			}
			info, err := os.Stat(filepath.Join(dir, "resources.go"))
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("application declaration permissions changed")
			}
			child, err := os.ReadFile(filepath.Join(dir, "sql/original-child.sql"))
			if err != nil || string(child) != "SELECT ID FROM original_child" {
				t.Fatal("original linked resource lost")
			}
			if !stale {
				command := exec.Command("go", "test", "-mod=mod", "./records")
				command.Dir = root
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("retained filesystem and generated contracts do not compile: %v\n%s", err, output)
				}
			}
			if stale {
				if _, err = os.Stat(filepath.Join(dir, plan.Input.Destination)); !os.IsNotExist(err) {
					t.Fatal("failed admission published generated contracts")
				}
			}
		})
	}
}
