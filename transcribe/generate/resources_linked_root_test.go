package generate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type linkedRootResourceRow struct {
	Id int `sqlx:"id"`
}
type linkedRootResourceOutput struct {
	Data []*linkedRootResourceRow `parameter:"Data,kind=output,in=view" view:"parents,table=parents" sql:"uri=queries/parents.sql"`
}

func TestLinkedRootSQLDestinationRetainsCompiledURI(t *testing.T) {
	const pkg = "example.com/generated/parents"
	const query = "SELECT id FROM parents"
	for _, tc := range []struct{ name, destination, namespace string }{
		{name: "implicit"},
		{name: "matching", destination: "queries/parents.sql"},
		{name: "conflicting", destination: "queries/renamed.sql"},
		{name: "qualified implicit", namespace: readableResourceNamespace(pkg, "Parents")},
		{name: "qualified matching", destination: "queries/parents.sql", namespace: readableResourceNamespace(pkg, "Parents")},
		{name: "foreign namespace", namespace: "foreign"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			destination := tc.destination
			outputType := reflect.TypeFor[linkedRootResourceOutput]()
			expectedURI := "uri=queries/parents.sql"
			if tc.namespace != "" {
				field, _ := outputType.FieldByName("Data")
				expectedURI = "uri=" + tc.namespace + ":queries/parents.sql"
				field.Tag = reflect.StructTag(strings.Replace(string(field.Tag), "uri=queries/parents.sql", expectedURI, 1))
				outputType = reflect.StructOf([]reflect.StructField{field})
			}
			catalog := typecatalog.NewCatalog()
			row := x.NewType(reflect.TypeFor[linkedRootResourceRow](), x.WithName("Row"), x.WithPkgPath(pkg))
			output := x.NewType(outputType, x.WithName("Output"), x.WithPkgPath(pkg))
			if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, output); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Name: "Parents", RootView: &spec.View{Name: "parents", TypeName: "Row", Source: &spec.ViewSource{SQL: query, Table: "parents"}, Columns: []*spec.Column{{Name: "Id", Source: "id", Type: spec.TypeRef{Name: "int"}}}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}}
			if destination != "" {
				component.Settings = &spec.Settings{Generation: &spec.GenerationSettings{SQLFiles: map[string]string{"parents": destination}}}
			}
			plan, err := New(Input{Component: component, TargetPackage: pkg, PackageName: "parents", TypeResolver: resolver, SQLResources: true, Contracts: ContractReferences{Output: &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}}, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}}).Plan()
			if tc.namespace == "foreign" {
				if err == nil || !strings.Contains(err.Error(), "SQL namespace") {
					t.Fatalf("foreign namespace accepted: %v", err)
				}
			} else if destination == "queries/renamed.sql" {
				if err == nil || !strings.Contains(err.Error(), "differs from uneditable URI") {
					t.Fatalf("compiled URI override accepted: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, file := range plan.Resources.Files {
					if file.Path == "queries/parents.sql" && file.Content == query {
						found = true
					}
					if file.Path == "sql/parents.sql" {
						t.Fatal("linked SQL moved to default destination")
					}
				}
				if !found {
					t.Fatalf("compiled SQL destination lost: %#v", plan.Resources.Files)
				}
				if plan.Generation.SQLFile("parents", "") != "queries/parents.sql" {
					t.Fatal("plan does not retain compiled destination")
				}
			}
			if component.Settings == nil {
				if destination != "" {
					t.Fatal("authored settings lost")
				}
			} else if component.Settings.Generation.SQLFiles["parents"] != destination {
				t.Fatal("planning changed authored destination")
			}
			field, _ := outputType.FieldByName("Data")
			if field.Tag.Get("sql") != expectedURI || component.RootView.Source.SQL != query {
				t.Fatal("planning changed authored SQL contract")
			}
		})
	}
}
