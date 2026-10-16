package generate

import (
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type reviewLinkedRow struct{ ID int }
type reviewLinkedInput struct{}
type reviewLinkedOutput struct {
	Data []*reviewLinkedRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func TestReviewUnmarkedLinkedGETWithAuthoredResources(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	pkg := "example.com/generated/records"
	dir := filepath.Join(root, "records")
	if err := os.MkdirAll(filepath.Join(dir, "sql"), 0755); err != nil {
		t.Fatal(err)
	}
	authored := `package records
import "embed"
const RecordsDatlyResourceNamespace = "` + readableResourceNamespace(pkg, "Records") + `"
//go:embed "sql/records.sql"
var RecordsDatlyResources embed.FS
var OriginalFS = RecordsDatlyResources
`
	for file, body := range map[string]string{"resources.go": authored, "sql/records.sql": "SELECT ID FROM records"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	catalog := typecatalog.NewCatalog()
	row := x.NewType(reflect.TypeFor[reviewLinkedRow](), x.WithName("Row"), x.WithPkgPath(pkg))
	input := x.NewType(reflect.TypeFor[reviewLinkedInput](), x.WithName("Input"), x.WithPkgPath(pkg))
	output := x.NewType(reflect.TypeFor[reviewLinkedOutput](), x.WithName("Output"), x.WithPkgPath(pkg))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, input, output); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}, Name: "Records", Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}, RootView: &spec.View{Name: "records", TypeName: "Row", Source: &spec.ViewSource{SQL: "SELECT ID FROM records", Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}}}}}
	plan, err := New(Input{Component: component, TargetPackage: pkg, PackageName: "records", ProjectRoot: root, TypeResolver: resolver, SQLResources: true, Contracts: ContractReferences{Input: &ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}}, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Ownership != ContractLinked || plan.Output.Ownership != ContractLinked || !plan.Resources.authoredSource() {
		t.Fatal("fixture did not exercise linked+linked authored resources")
	}
	actual, err := os.ReadFile(filepath.Join(dir, "resources.go"))
	if err != nil || string(actual) != authored {
		t.Fatal("authored resources mutated")
	}
	t.Log("ordinary unmarked linked Input+Output GET retained authored resource declaration")
}
