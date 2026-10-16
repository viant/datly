package handlergen

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestFactoryLinkedCurrentCanonicalProjectionAuthority(t *testing.T) {
	const local, foreign = "example.com/app/local", "example.com/app/foreign"
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(struct{ Value string }{}), x.WithName("Record"), x.WithPkgPath(local))))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(struct{ Competing int }{}), x.WithName("Record"), x.WithPkgPath(foreign))))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: foreign})
	require.NoError(t, err)
	bare, err := resolver.ResolveShape("Record")
	require.NoError(t, err)
	require.Equal(t, foreign+".Record", bare.Descriptor.Key(), "bare lookup is resolvable but has competing authority")
	for _, tc := range []struct{ name, key, want string }{
		{name: "linked canonical selection", key: local + ".Record"},
		{name: "missing canonical cannot fall back", key: "example.com/app/missing.Record", want: "was not resolved"},
		{name: "empty canonical cannot fall back", want: "requires a canonical descriptor key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generated := &gen.Plan{Views: []gen.ViewPlan{{Name: "Record", Type: "Record", Ownership: gen.ViewLinked, DescriptorKey: tc.key}}}
			generation := New(nil, &gen.Input{TargetPackage: local, TypeResolver: resolver}, Options{})
			fields, err := generation.currentProjectionFields(generated, "[]*Record", spec.CardinalityMany)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			require.Len(t, fields, 1)
			require.Equal(t, "Value", fields[0].name)
			require.Equal(t, "string", fields[0].expression)
		})
	}
}
