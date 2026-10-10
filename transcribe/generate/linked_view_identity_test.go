package generate

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestLinkedViewTypeRetainsImportedPackageIdentity(t *testing.T) {
	const pkg = "example.com/app/model"
	for _, tc := range []struct {
		expression string
		accepted   bool
	}{
		{"Row", true}, {"model.Row", true}, {pkg + ".Row", true},
		{"foreign.Row", false}, {"unknown.Row", false}, {"model.Other", false}, {"*model.Row", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeFor[linkedResourceChild](), x.WithName("Row"), x.WithPkgPath(pkg))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			imports := []typecatalog.PackageImport{{Alias: "model", Package: pkg}, {Alias: "foreign", Package: "example.com/other/model"}}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/lookup", Imports: imports})
			if err != nil {
				t.Fatal(err)
			}
			view := &spec.View{Name: "rows", TypeName: tc.expression, Source: &spec.ViewSource{SQL: "SELECT id,parent_id,label FROM rows"}}
			before, _ := json.Marshal(view)
			component := &spec.Component{Name: "Lookup", RootView: view, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: pkg}, {Alias: "foreign", Package: "example.com/other/model"}}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*model.Row"}}}
			plan, err := New(Input{Component: component, TargetPackage: "example.com/app/lookup", TypeResolver: resolver, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
			if (err == nil) != tc.accepted {
				t.Fatalf("accepted=%v err=%v", tc.accepted, err)
			}
			after, _ := json.Marshal(view)
			if string(before) != string(after) {
				t.Fatal("linked view metadata changed")
			}
			if tc.accepted && (len(plan.Views) != 1 || plan.Views[0].Ownership != ViewLinked || plan.Views[0].Package != pkg || plan.Views[0].Type != "model.Row") {
				t.Fatalf("linked authority lost: %+v", plan.Views)
			}
		})
	}
}
