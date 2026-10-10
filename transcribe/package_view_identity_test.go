package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestIndependentLinkedViewTypeUsesDeclaredImports(t *testing.T) {
	const pkg = "example.com/app/model"
	for _, tc := range []struct {
		expression string
		accepted   bool
	}{
		{"Row", true}, {"model.Row", true}, {pkg + ".Row", true}, {"other.Row", false}, {"unknown.Row", false}, {"model.Other", false}, {"*model.Row", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeFor[PackageCompileView](), x.WithName("Row"), x.WithPkgPath(pkg))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/lookup", Imports: []typecatalog.PackageImport{{Alias: "model", Package: pkg}, {Alias: "other", Package: "example.com/other/model"}}})
			if err != nil {
				t.Fatal(err)
			}
			view := &spec.View{Name: "rows", TypeName: tc.expression, Source: &spec.ViewSource{SQL: "SELECT id FROM rows"}}
			component := &spec.Component{Views: []*spec.View{view}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: pkg}, {Alias: "other", Package: "example.com/other/model"}}}}
			target := map[string]linkedInputView{}
			for _, current := range []*typecatalog.Resolver{resolver, nil} {
				err = newPackageViewResolver(component, current).add(target, "rows", descriptor)
				if (err == nil) != tc.accepted {
					t.Fatalf("resolver=%v accepted=%v err=%v", current != nil, tc.accepted, err)
				}
			}
			if (err == nil) != tc.accepted {
				t.Fatalf("accepted=%v err=%v", tc.accepted, err)
			}
			if tc.accepted && len(target) != 1 {
				t.Fatalf("linked view missing: %+v", target)
			}
			if view.TypeName != tc.expression || view.Source.SQL != "SELECT id FROM rows" {
				t.Fatal("original contract changed")
			}
		})
	}
}
