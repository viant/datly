package link

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestAuthoredDeclarationOwnsWhenOnRegeneration(t *testing.T) {
	for _, guard := range []string{"", "PayloadKeyedViews", "OtherGuard"} {
		t.Run("guard="+guard, func(t *testing.T) {
			base := &spec.Component{Parameters: []*spec.Parameter{
				{Name: "CurOrderAudience", Source: spec.BindSource{Kind: "view", Name: "curOrderAudience"}, When: "PayloadKeyedViews"},
				{Name: "Untouched", Source: spec.BindSource{Kind: "view", Name: "untouched"}, When: "RetainedGuard"},
			}}
			authored := &spec.Component{Parameters: []*spec.Parameter{
				{Name: "CurOrderAudience", Source: spec.BindSource{Kind: "view", Name: "curOrderAudience"}, When: guard},
			}}
			before := base.Clone()
			got, err := (&Loader{PackageComponent: base, AuthoredComponent: authored, DQLAuthored: true}).Load()
			require.NoError(t, err)
			require.Len(t, got.Parameters, 2)
			require.Equal(t, guard, got.Parameters[0].When, "removed authored guard must not survive from generated Go")
			require.Equal(t, "RetainedGuard", got.Parameters[1].When, "unmatched package parameter retains its policy")
			require.Equal(t, before, base.Clone())
			again, err := (&Loader{PackageComponent: got, AuthoredComponent: authored, DQLAuthored: true}).Load()
			require.NoError(t, err)
			require.Equal(t, got.Parameters, again.Parameters, "regeneration must be stable")
		})
	}
}
