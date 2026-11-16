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

func TestContractLinkerDistinguishesInferredAndAuthoredSourceTable(t *testing.T) {
	authored := &spec.Component{RootView: &spec.View{Name: "users", Source: &spec.ViewSource{SQL: "SELECT ID FROM users"}}}
	compiled := authored.Clone()
	compiled.RootView.Source.Table = "users"
	compiled.RootView.Source.TableInferred = true
	linker := &Contracts{PackageComponent: authored, CompiledComponent: compiled}
	require.True(t, linker.equalViews(), "derived physical table does not change the authored SQL contract")
	require.Equal(t, "users", compiled.RootView.Source.Table, "comparison must preserve compiled metadata")
	authored.RootView.Source.Table = "users"
	require.True(t, linker.equalViews(), "matching explicit and inferred tables retain linked ownership")
	authored.RootView.Source.Table = "different_users"
	require.False(t, linker.equalViews(), "inference must not erase conflicting explicit table authority")
	authored.RootView.Source.Table = ""
	compiled.RootView.Source.SQL = "SELECT ID FROM other_users"
	require.False(t, linker.equalViews(), "changed executable SQL must not link")
	compiled.RootView.Source.SQL = "SELECT ID FROM users"
	compiled.RootView.Source.TableInferred = false
	require.False(t, linker.equalViews(), "an explicit table declaration remains contract authority")
	authored.RootView.Source.Table = "users"
	require.True(t, linker.equalViews())
	compiled.RootView.Source.Table = "other_users"
	require.False(t, linker.equalViews(), "conflicting explicit source tables must not link")
	authored.RootView.Source.SQL = ""
	compiled.RootView.Source.SQL = ""
	compiled.RootView.Source.TableInferred = true
	require.False(t, linker.equalViews(), "table-only sources retain their physical table")
	require.Equal(t, "users", authored.RootView.Source.Table)
	require.Equal(t, "other_users", compiled.RootView.Source.Table)
}

func TestContractLinkerNormalizesNestedInferredTableWithoutMutation(t *testing.T) {
	authored := &spec.Component{RootView: &spec.View{Name: "parents", Relations: []*spec.Relation{{Name: "children", View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT ID FROM children"}}}}}}
	compiled := authored.Clone()
	source := compiled.RootView.Relations[0].View.Source
	source.Table, source.TableInferred = "children", true
	for _, pair := range [][2]*spec.Component{{authored, compiled}, {compiled, authored}} {
		linker := &Contracts{PackageComponent: pair[0], CompiledComponent: pair[1]}
		require.True(t, linker.equalViews())
		require.Equal(t, "children", source.Table)
		require.Empty(t, authored.RootView.Relations[0].View.Source.Table)
	}
	source.SQL = "SELECT ID FROM other_children"
	require.False(t, (&Contracts{PackageComponent: authored, CompiledComponent: compiled}).equalViews())
}
