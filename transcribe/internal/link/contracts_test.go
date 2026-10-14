package link

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestContractLinkerNormalizesInferredSelectorPolicy(t *testing.T) {
	authored := &spec.Component{Name: "Records", RootView: &spec.View{Name: "rows"}, Parameters: []*spec.Parameter{{Name: "OrderBy", QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyOrderBy}}}}
	compiled := authored.Clone()
	require.NoError(t, compiled.RootView.EnableQuerySelector(spec.SelectorPropertyOrderBy))
	linker := &Contracts{PackageComponent: authored, CompiledComponent: compiled}
	require.True(t, linker.equalViews())
	require.Nil(t, authored.RootView.Selector, "comparison must not mutate authored metadata")
	require.NoError(t, compiled.RootView.Selector.SetPermission(spec.SelectorPropertyOrderBy, false))
	require.False(t, linker.equalViews(), "an authored permission change must remain a contract change")
}
