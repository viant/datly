package transcribe

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/testdata/linkedpredicate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestGoComponentPredicateDependencies(t *testing.T) {
	for _, tc := range []struct {
		name, reference, wantError string
		imported, embedded         bool
		explicit, existing         reflect.Type
	}{
		{name: "linked tag only", reference: predicateTestPackage + ".Threshold"},
		{name: "declaring file alias", reference: "security.Threshold", imported: true},
		{name: "embedded declaring alias", reference: "security.Threshold", imported: true, embedded: true},
		{name: "registry precedence", reference: predicateTestPackage + ".Threshold", explicit: reflect.TypeFor[availablePredicate]()},
		{name: "conflict", reference: predicateTestPackage + ".Threshold", explicit: reflect.TypeFor[availablePredicate](), existing: reflect.TypeFor[linkedpredicate.Threshold](), wantError: "different compiled identity"},
		{name: "incompatible", reference: predicateTestPackage + ".Threshold", explicit: reflect.TypeFor[incompatiblePredicate](), wantError: "does not implement predicate.Handler"},
		{name: "missing", reference: predicateTestPackage + ".Absent", wantError: "was not found"},
		{name: "unlinked", reference: predicateTestPackage + ".SourceOnly", wantError: "not linked into this binary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := predicateDiscoveryWorkspace(t)
			writeSourceFile(t, base, "linkedpredicate/unlinked.go", "package linkedpredicate\ntype SourceOnly struct{}\n")
			if !tc.imported {
				// A predicate-only dependency must not activate this invalid holder
				// or attempt to load its missing resource.
				writeSourceFile(t, base, "linkedpredicate/unrelated.go", "package linkedpredicate\nimport xdatly \"github.com/viant/xdatly\"\ntype Bad struct { Read xdatly.Component[struct{},struct{}] `component:\"Invalid\"` }\n")
				writeSourceFile(t, base, "linkedpredicate/.datly-gen.json", `{"version":5,"owner":"Other","resources":{"namespace":"unused","files":["missing.sql"]}}`)
			}
			imports := ""
			if tc.imported {
				imports = "import security \"" + predicateTestPackage + "\"\n"
			}
			writeSourceFile(t, base, "contracts/input.go", "package contracts\n"+imports+"type Input struct { Minimum int `parameter:\"Minimum,kind=query,in=min\" predicate:\"handler,"+tc.reference+"\"` }\n")
			input := "contracts.Input"
			embedded := ""
			if tc.embedded {
				input = "Input"
				embedded = "type Input struct { contracts.Input }\n"
			}
			writeSourceFile(t, base, "reader/component.go", "package reader\nimport ( xdatly \"github.com/viant/xdatly\"; contracts \""+predicateTestModule+"/contracts\" )\n"+embedded+"type Output struct{}\ntype Holder struct { Read xdatly.Component["+input+",Output] `component:\"Read,path=/records,method=GET\"` }\n")
			catalog := typecatalog.NewCatalog()
			registry := x.NewRegistry()
			if tc.existing != nil {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(tc.existing)))
			}
			if tc.explicit != nil {
				registry.Register(x.NewType(tc.explicit, x.WithPkgPath(predicateTestPackage), x.WithName("Threshold")))
			}
			before, err := catalog.Clone()
			require.NoError(t, err)
			project, err := (&Discovery{BaseDir: base, Include: []string{predicateTestModule + "/reader"}, Types: catalog, Registry: registry}).Compile(context.Background())
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
				require.Len(t, project.Components, 1)
				result := project.Components[0]
				require.Equal(t, predicateTestPackage+".Threshold", result.Component.Parameters[0].Predicates[0].Args[0])
				descriptor, found, err := result.Source.Types.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
				require.NoError(t, err)
				require.True(t, found)
				want := tc.explicit
				if want == nil {
					want = reflect.TypeFor[linkedpredicate.Threshold]()
				}
				require.Equal(t, want, descriptor.Type)
				require.NotNil(t, descriptor.SynteticType.TypeSpec)
			}
			old, oldFound, err := before.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
			require.NoError(t, err)
			current, currentFound, err := catalog.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
			require.NoError(t, err)
			require.Equal(t, oldFound, currentFound)
			require.Equal(t, old, current)
		})
	}
}
