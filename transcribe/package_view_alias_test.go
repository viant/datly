package transcribe

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/internal/link"
	"testing"
)

func TestPackageViewTypesCanonicalBeforeDiscovery(t *testing.T) {
	const local = "example.com/app/audience/patch"
	for _, tc := range []struct{ name, foreign, want string }{
		{"actual package name differs from DQL alias", "", local},
		{"explicit foreign alias wins", "example.com/foreign", "example.com/foreign"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &spec.Component{TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "acpatch", Package: local}}}, Parameters: []*spec.Parameter{
				{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "current"}, TypeExpr: "[]*patch.Creative", OutputTypeExpr: "[]*patch.Creative"},
				{Name: "Body", Source: spec.BindSource{Kind: "body"}, TypeExpr: "[]*patch.Creative"},
			}}
			if tc.foreign != "" {
				base.TypeContext.Imports = append(base.TypeContext.Imports, spec.ImportSpec{Alias: "patch", Package: tc.foreign})
			}
			authority := &packageAuthority{route: &bootstrap.RouteSource{PackagePath: local, PackageName: "patch"}}
			detached := base.Clone()
			require.NoError(t, authority.canonicalPackageViewTypes(detached))
			require.Equal(t, "[]*"+tc.want+".Creative", detached.Parameters[0].TypeExpr)
			require.Equal(t, "[]*"+tc.want+".Creative", detached.Parameters[0].OutputTypeExpr)
			require.Equal(t, "[]*patch.Creative", base.Parameters[0].TypeExpr, "caller metadata stays unchanged")
			require.Equal(t, "[]*patch.Creative", detached.Parameters[1].TypeExpr, "only independent reads are normalized")
			authored := &spec.Component{Parameters: []*spec.Parameter{{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "current"}, TypeExpr: "[]*acpatch.Replacement", OutputTypeExpr: "[]*acpatch.Replacement"}}}
			merged, err := (&link.Loader{PackageComponent: detached, AuthoredComponent: authored, DQLAuthored: true}).Load()
			require.NoError(t, err)
			require.Equal(t, "[]*acpatch.Replacement", merged.Parameters[0].TypeExpr, "DQL overlay remains authoritative")
			require.Equal(t, "[]*acpatch.Replacement", merged.Parameters[0].OutputTypeExpr)
		})
	}
}
