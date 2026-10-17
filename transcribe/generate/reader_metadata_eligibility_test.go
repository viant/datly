package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type reviewIndependentInput struct {
	Authorized []*reviewLinkedRow `parameter:"Authorized,kind=view,in=Authorization" view:"Authorization,table=records" sql:"uri=sql/authorization.sql"`
}

// The real test-package nominal identities avoid substituting generated record
// shapes while exercising same-package linked contracts and native resources.
func reviewLinkedEligibilityInput(t *testing.T, independent bool) Input {
	t.Helper()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/transcribe"}).Write(t, root)
	pkg := reflect.TypeFor[reviewLinkedRow]().PkgPath()
	directory := filepath.Join(root, "generate")
	if err := os.MkdirAll(filepath.Join(directory, "sql"), 0700); err != nil {
		t.Fatal(err)
	}
	namespace := readableResourceNamespace(pkg, "Records")
	declarations := generatedHeader("Records") + `package generate
import "embed"
const RecordsDatlyResourceNamespace = "` + namespace + `"
//go:embed "sql/records.sql" "sql/authorization.sql"
var RecordsDatlyResources embed.FS
`
	for filename, body := range map[string]string{"records_resources.go": declarations, "sql/records.sql": "SELECT ID FROM records", "sql/authorization.sql": "SELECT ID FROM records"} {
		if err := os.WriteFile(filepath.Join(directory, filename), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	row := x.NewType(reflect.TypeFor[reviewLinkedRow]())
	input := x.NewType(reflect.TypeFor[reviewLinkedInput]())
	if independent {
		input = x.NewType(reflect.TypeFor[reviewIndependentInput]())
	}
	output := x.NewType(reflect.TypeFor[reviewLinkedOutput]())
	catalog := typecatalog.NewCatalog()
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, input, output); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}, Name: "Records", Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + row.Key()}}, RootView: &spec.View{Name: "records", TypeName: row.Name, Source: &spec.ViewSource{SQL: "SELECT ID FROM records", Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}}}}}
	views := ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}
	if independent {
		authorization := &spec.View{Name: "Authorization", TypeName: row.Name, Source: &spec.ViewSource{SQL: "SELECT ID FROM records", Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}}}}
		component.Views = []*spec.View{authorization}
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Authorized", Source: spec.BindSource{Kind: "view", Name: "Authorization"}, TypeExpr: "[]*" + row.Key()})
		identity, err := authorization.Identity()
		if err != nil {
			t.Fatal(err)
		}
		views[identity] = &ViewReference{DescriptorKey: row.Key()}
	}
	resources := resource.New()
	if err := resources.Register(namespace, os.DirFS(directory)); err != nil {
		t.Fatal(err)
	}
	return Input{Component: component, TargetPackage: pkg, PackageName: "generate", ProjectRoot: root, TypeResolver: resolver, Resources: resources, SQLResources: true, Contracts: ContractReferences{Input: &ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}}, Views: views}
}
func TestReviewUnmarkedLinkedGETWithIndependentView(t *testing.T) {
	input := reviewLinkedEligibilityInput(t, true)
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Ownership != ContractLinked || plan.Output.Ownership != ContractLinked {
		t.Fatal("fixture did not retain both linked contracts")
	}
	if len(input.Component.Views) != 1 || len(plan.Views) < 2 {
		t.Fatal("independent view lost")
	}
	for _, file := range plan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			t.Fatal("unsupported unmarked independent reader unexpectedly selected metadata")
		}
	}
	t.Log("unmarked linked+linked reader retained independent input view")
}
func TestReviewPureMetadataReaderWithoutSelector(t *testing.T) {
	input := reviewLinkedEligibilityInput(t, false)
	if input.Component.SelectorCompanion != nil {
		t.Fatal("fixture unexpectedly has selector companion")
	}
	// Component has no selector companion or query selector parameters. A bounded
	// graph control still needs the complete metadata transport on native reload.
	limit := 3
	input.Component.RootView.Source.Controls = &spec.ViewControls{Limit: &limit}
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	metadata := false
	for _, file := range plan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			metadata = true
			if !strings.Contains(file.Content, `"limit": 3`) {
				t.Fatal("canonical limit missing from metadata")
			}
		}
	}
	expected := true
	if metadata != expected {
		t.Fatalf("metadata emitted=%v expected=%v without selector", metadata, expected)
	}
	if plan.Input.Ownership != ContractLinked || plan.Output.Ownership != ContractLinked {
		t.Fatal("fixture lost linked contract ownership")
	}
	t.Logf("pure reader without selector metadata emitted=%v", metadata)
}
