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

func TestMutationInputLinkTracksRootLifecycle(t *testing.T) {
	for _, kind := range []string{"body", "internal"} {
		t.Run(kind, func(t *testing.T) {
			param := &spec.Parameter{Name: "Rows"}
			param.Source.Kind = kind
			prior := &spec.Component{RootView: &spec.View{Name: "Rows"}, Parameters: []*spec.Parameter{param}}
			next := prior.Clone()
			link := Contracts{PackageComponent: prior, CompiledComponent: next}
			require.True(t, link.EqualParams(false))
			next.RootView.EntityHooks = "ObservedRows"
			require.False(t, link.EqualParams(false), "adding a lifecycle requires fresh input tags")
			prior.RootView.EntityHooks = next.RootView.EntityHooks
			require.True(t, link.EqualParams(false))
			next.RootView.EntityHooks = ""
			require.False(t, link.EqualParams(false), "removing a lifecycle must retire its input tag")
		})
	}
}
