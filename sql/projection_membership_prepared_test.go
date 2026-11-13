package sql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
)

func TestPreparedMembershipUsesCheckedNativeMappings(t *testing.T) {
	view := staticTestView()
	source := `SELECT r.ID,r.NAME FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(99,"AND")).Build("WHERE")}`
	prepared, err := (SelectorProjection{SQL: source, View: view}).PrepareMembership()
	require.NoError(t, err)
	for iteration := 0; iteration < 3; iteration++ {
		selected, err := prepared.Columns([]string{"nAmE", "id"})
		require.NoError(t, err)
		require.Equal(t, "ID", selected[0].OutputName(), "explicit SELECT order retains native semantics")
		require.Equal(t, "NAME", selected[1].OutputName())
		selected[0] = ProjectionColumn{}
		_, err = prepared.Columns([]string{"MiSsInG"})
		var unknown *UnknownProjectionColumnError
		require.ErrorAs(t, err, &unknown)
		require.Equal(t, "MiSsInG", unknown.RequestedColumn)
	}
	view.SelectorFields = []data.SelectorField{{GoName: "DisplayName", PublicName: "displayName", Column: "NAME", SourceAlias: "legacy", SourceAliasColumns: []string{"NAME"}}}
	prepared, err = (SelectorProjection{SQL: source, View: view}).PrepareMembership()
	require.NoError(t, err)
	require.True(t, prepared.SourceOnlySelection("legacy"))
	require.False(t, prepared.SourceOnlySelection("DisplayName"))
	_, err = prepared.Columns([]string{"legacy"})
	require.NoError(t, err)
}

func TestPreparedMembershipDefersDynamicProjection(t *testing.T) {
	_, err := (SelectorProjection{SQL: `SELECT ID #if($Unsafe.IncludeName),NAME #end FROM records`, View: staticTestView()}).PrepareMembership()
	require.Error(t, err)
}

func TestResponseFieldSelectionConcurrentNativeProof(t *testing.T) {
	view := &data.View{SelectorFields: []data.SelectorField{{GoName: "Label", PublicName: "label", Column: "label", SourceAlias: "legacy", SourceAliasColumns: []string{"label"}}}}
	projection := SelectorProjection{SQL: `SELECT ID,'visible' AS label FROM records`, View: view}
	selection := NewResponseFieldSelection([]string{"legacy", "ID"})
	require.False(t, selection.Evaluated())
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := selection.Validate(projection); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	require.True(t, selection.Evaluated())
	sourceOnly, err := selection.SourceOnlySelection("legacy")
	require.NoError(t, err)
	require.True(t, sourceOnly)
	// A dynamic sibling now projects the alias as an actual output. Even when
	// both queries accept the field, batches must agree about its mask identity.
	err = selection.Validate(SelectorProjection{SQL: `SELECT ID,'visible' AS label,'public' AS legacy FROM records`, View: view})
	var consistency *ResponseProjectionConsistencyError
	require.ErrorAs(t, err, &consistency)
	require.Equal(t, "legacy", consistency.Column)
	err = selection.Validate(SelectorProjection{SQL: `SELECT ID FROM records`, View: view})
	var unknown *UnknownProjectionColumnError
	require.ErrorAs(t, err, &unknown)
}
