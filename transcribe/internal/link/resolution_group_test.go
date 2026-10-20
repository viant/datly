package link

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestOverlayResolutionGroup(t *testing.T) {
	old := &spec.ResolutionGroupSpec{Name: "old", Mode: "dependencies", After: []string{"Auth"}, DependsOn: []string{"Other"}}
	for _, tc := range []struct {
		name           string
		base, authored *spec.ResolutionGroupSpec
	}{
		{name: "add", authored: old},
		{name: "rename", base: old, authored: &spec.ResolutionGroupSpec{Name: "subject", Mode: "dependencies"}},
		{name: "mode and prerequisites", base: old, authored: &spec.ResolutionGroupSpec{Name: "old", After: []string{"Body"}, DependsOn: []string{"First"}}},
		{name: "clear barriers and prerequisites", base: old, authored: &spec.ResolutionGroupSpec{Name: "old", Mode: "dependencies"}},
		{name: "remove", base: old},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &spec.Parameter{Name: "Child", ResolutionGroup: tc.base}
			authored := &spec.Parameter{Name: "Child", ResolutionGroup: tc.authored}
			baseBefore, authoredBefore := base.Clone(), authored.Clone()
			got := overlayParam(base, authored)
			require.Equal(t, tc.authored, got.ResolutionGroup)
			require.Equal(t, got, overlayParam(got, authored), "repeated overlay must be stable")
			if got.ResolutionGroup != nil {
				got.ResolutionGroup.Name = "changed"
				if len(got.ResolutionGroup.After) > 0 {
					got.ResolutionGroup.After[0] = "changed"
				}
				if len(got.ResolutionGroup.DependsOn) > 0 {
					got.ResolutionGroup.DependsOn[0] = "changed"
				}
			}
			require.Equal(t, baseBefore, base.Clone(), "base must remain detached")
			require.Equal(t, authoredBefore, authored.Clone(), "authored slices must remain detached")
		})
	}
}

func TestResolutionGroupUnmatchedParameterRetained(t *testing.T) {
	base := &spec.Component{Parameters: []*spec.Parameter{{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/child"}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "subject", Mode: "dependencies", After: []string{"Auth"}}}}}
	before := base.Clone()
	got, err := (&Loader{PackageComponent: base, AuthoredComponent: &spec.Component{Parameters: []*spec.Parameter{{Name: "Other", Source: spec.BindSource{Kind: "query", Name: "other"}}}}, DQLAuthored: true}).Load()
	require.NoError(t, err)
	require.Equal(t, base.Parameters[0].Clone(), got.Parameters[1])
	got.Parameters[1].ResolutionGroup.After[0] = "changed"
	require.Equal(t, before, base.Clone())
}
