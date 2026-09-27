package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/testdata/linkedpredicate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestLinkedPredicateAuthority(t *testing.T) {
	const pkg = "github.com/viant/datly/transcribe/testdata/linkedpredicate"
	for _, tc := range []struct {
		name, reference, wantError string
		explicit                   reflect.Type
		sourceOnly                 bool
	}{
		{name: "linked", reference: pkg + ".Threshold"},
		{name: "explicit precedence", reference: pkg + ".Threshold", explicit: reflect.TypeFor[aliasOrderScope]()},
		{name: "incompatible authority", reference: pkg + ".Threshold", explicit: reflect.TypeFor[aliasWrongSignature](), wantError: "does not implement predicate.Handler"},
		{name: "missing", reference: pkg + ".Missing", wantError: "was not found"},
		{name: "source only", reference: pkg + ".SourceOnly", sourceOnly: true, wantError: "not linked into this binary"},
		{name: "no alias guessing", reference: "linkedpredicate.Threshold", wantError: "was not found"},
		{name: "no global short name search", reference: "Threshold", wantError: "was not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if tc.sourceOnly {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: pkg, Name: "SourceOnly", Definition: "struct{}"}))
			}
			registry := x.NewRegistry()
			if tc.explicit != nil {
				registry.Register(x.NewType(tc.explicit, x.WithPkgPath(pkg), x.WithName("Threshold")))
			}
			builder, err := NewArtifactBuilder(registry)
			require.NoError(t, err)
			component := &spec.Component{
				Key: spec.Key{Scope: "example.com/reader"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Parameters: []*spec.Parameter{{Name: "Minimum", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "min"}, Predicates: []*spec.Predicate{{Name: "handler", Args: []string{tc.reference}}}}},
			}
			before := component.Clone()
			_, err = builder.Build(ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{ Minimum int }](), Types: catalog})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, before, component.Clone())
			_, found, err := catalog.Resolve(typecatalog.PackageAuthority, pkg+".Threshold")
			require.NoError(t, err)
			require.False(t, found, "never mutate supplied authority")
		})
	}
	// Keep the linked implementation alive without registering it in a catalog.
	require.NotNil(t, reflect.TypeFor[linkedpredicate.Threshold]())
}

func TestLinkedPredicateCatalogIsolationConcurrent(t *testing.T) {
	const name = "github.com/viant/datly/transcribe/testdata/linkedpredicate.Threshold"
	catalog := typecatalog.NewCatalog()
	for i := 0; i < 8; i++ {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Minimum", Predicates: []*spec.Predicate{{Name: "handler", Args: []string{name}}}}}}
			linked, err := linkPredicateTypes(catalog, component, nil)
			require.NoError(t, err)
			typ, found, err := linked.ResolveRuntimeType(typecatalog.PackageAuthority, name)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, reflect.TypeFor[linkedpredicate.Threshold](), typ)
			_, found, err = catalog.ResolveRuntimeType(typecatalog.PackageAuthority, name)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}
