package bootstrap

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type aliasOrderScope struct{}

func (*aliasOrderScope) Compute(context.Context, any) (*xpredicate.Criteria, error) { return nil, nil }

type aliasWrongSignature struct{}

func (*aliasWrongSignature) Compute(context.Context, string) (*xpredicate.Criteria, error) {
	return nil, nil
}

func TestArtifactPredicateTypeAuthority(t *testing.T) {
	type input struct{ Value int }
	const security = "example.com/app/orders/security"
	const other = "example.com/app/other/security"
	catalog := typecatalog.NewCatalog()
	for _, item := range []struct {
		pkg, name string
		typ       reflect.Type
		origin    typecatalog.TypeOrigin
	}{
		{security, "OrderScope", reflect.TypeOf(aliasOrderScope{}), typecatalog.TypeOriginPackage},
		// Runtime package authority must win over a same-key DQL descriptor.
		{security, "OrderScope", reflect.TypeOf(aliasWrongSignature{}), typecatalog.TypeOriginDQL},
		{other, "OrderScope", reflect.TypeOf(aliasWrongSignature{}), typecatalog.TypeOriginPackage},
		{other, "UnscopedOnly", reflect.TypeOf(aliasOrderScope{}), typecatalog.TypeOriginPackage},
		{other, "Unknown", reflect.TypeOf(aliasOrderScope{}), typecatalog.TypeOriginPackage},
	} {
		require.NoError(t, catalog.Register(item.origin, x.NewType(item.typ, x.WithPkgPath(item.pkg), x.WithName(item.name))))
	}
	registry, err := catalog.Registry(typecatalog.PackageAuthority)
	require.NoError(t, err)
	linkedBuilder, err := NewArtifactBuilder(registry)
	require.NoError(t, err)
	for _, mode := range []string{"catalog", "linked registry"} {
		t.Run(mode, func(t *testing.T) {
			for _, tc := range []struct {
				name, class           string
				args                  []string
				canonical, diagnostic string
			}{
				{name: "import alias", class: "handler", args: []string{"security.OrderScope"}, canonical: security + ".OrderScope"},
				{name: "explicit canonical", class: "handler", args: []string{security + ".OrderScope"}, canonical: security + ".OrderScope"},
				{name: "class normalization", class: " Handler ", args: []string{"security.OrderScope"}, canonical: security + ".OrderScope"},
				{name: "same short name other import", class: "handler", args: []string{"other.OrderScope"}, diagnostic: "does not implement predicate.Handler"},
				{name: "same short name canonical other package", class: "handler", args: []string{other + ".OrderScope"}, diagnostic: "does not implement predicate.Handler"},
				{name: "unknown alias", class: "handler", args: []string{"missing.OrderScope"}, diagnostic: `handler predicate type "missing.OrderScope" was not found`},
				{name: "unknown imported type", class: "handler", args: []string{"security.Unknown"}, diagnostic: `handler predicate type "` + security + `.Unknown" was not found`},
				{name: "no global short name fallback", class: "handler", args: []string{"UnscopedOnly"}, diagnostic: `handler predicate type "example.com/app/orders/read.UnscopedOnly" was not found`},
				{name: "missing argument", class: "handler", diagnostic: "requires an absolute type name"},
				{name: "empty argument", class: "handler", args: []string{" "}, diagnostic: "requires an absolute type name"},
				{name: "extra argument", class: "handler", args: []string{"security.OrderScope", "config"}, diagnostic: "accepts one type argument"},
				{name: "predicate class stays a string", class: "security.OrderScope", diagnostic: `predicate "security.OrderScope" is not registered`},
				{name: "builtin arguments stay strings", class: "contains", args: []string{"security", "OrderScope"}},
				{name: "builtin expression stays a string", class: "expr", args: []string{"security.OrderScope = 7"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					component := &spec.Component{
						Routes:      []*spec.Route{{Method: "GET", Path: "/orders"}},
						Key:         spec.Key{Scope: "example.com/app/orders/read"},
						TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "security", Package: security}, {Alias: "other", Package: other}}},
						Parameters:  []*spec.Parameter{{Name: "Value", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "value"}, Predicates: []*spec.Predicate{{Name: tc.class, Args: append([]string(nil), tc.args...)}}}},
					}
					before := component.Clone()
					artifactInput := ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), Types: catalog}
					var artifact *Artifact
					var err error
					if mode == "linked registry" {
						artifactInput.Types = nil
						artifact, err = linkedBuilder.Build(artifactInput)
					} else {
						artifact, err = BuildArtifact(artifactInput)
					}
					require.Equal(t, before, component.Clone(), "bootstrap must not mutate authored metadata")
					if tc.diagnostic != "" {
						require.ErrorContains(t, err, tc.diagnostic)
						if tc.class == "handler" {
							require.ErrorContains(t, err, "compile predicate handler for Value")
						}
						require.Nil(t, artifact)
						return
					}
					require.NoError(t, err)
					predicate := artifact.Component.Parameters[0].Predicates[0]
					require.Equal(t, tc.class, predicate.Name)
					expected := tc.args
					if tc.canonical != "" {
						expected = []string{tc.canonical}
					}
					require.Equal(t, expected, predicate.Args)
				})
			}
		})
	}
}
