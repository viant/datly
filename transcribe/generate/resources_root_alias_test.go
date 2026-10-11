package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestRootSQLAliasDoesNotStealSameNamedChildDestination(t *testing.T) {
	const pkg = "example.com/generated/parents"
	const rootSQL = "SELECT id FROM parents"
	for _, linkedOutput := range []bool{false, true} {
		t.Run(map[bool]string{false: "generated output", true: "linked output"}[linkedOutput], func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			row := x.NewType(reflect.TypeFor[linkedRootResourceRow](), x.WithName("Row"), x.WithPkgPath(pkg))
			output := x.NewType(reflect.TypeFor[linkedRootResourceOutput](), x.WithName("Output"), x.WithPkgPath(pkg))
			if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, output); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			destinations := map[string]string{"parentrows": "queries/parents.sql", "parents": "queries/children.sql"}
			component := &spec.Component{Name: "Parents", Settings: &spec.Settings{Generation: &spec.GenerationSettings{SQLFiles: destinations}}, RootView: &spec.View{Name: "parents", Namespace: "parentRows", TypeName: "Row", Source: &spec.ViewSource{SQL: rootSQL, Table: "parents"}, Columns: []*spec.Column{{Name: "Id", Source: "id", Type: spec.TypeRef{Name: "int"}}}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}}
			input := Input{Component: component, TargetPackage: pkg, PackageName: "parents", TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}}
			if linkedOutput {
				input.Contracts.Output = &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}
			}
			plan, err := New(input).Plan()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, file := range plan.Resources.Files {
				if file.Path == "queries/parents.sql" && file.Content == rootSQL {
					found = true
				}
				if file.Path == "queries/children.sql" {
					t.Fatal("root SQL occupied child destination")
				}
			}
			if !found {
				t.Fatal("explicit root alias destination lost")
			}
			if plan.Generation.SQLFile("parents", "") != "queries/children.sql" || destinations["parents"] != "queries/children.sql" || component.RootView.Namespace != "parentRows" {
				t.Fatal("child destination or authored alias changed")
			}
		})
	}
}
