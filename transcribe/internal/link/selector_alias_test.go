package link

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func selectorParam(name, source, view string, property spec.SelectorProperty) *spec.Parameter {
	return &spec.Parameter{
		Name:          name,
		Source:        spec.BindSource{Kind: "query", Name: source},
		QuerySelector: &spec.QuerySelectorBinding{View: view, Property: property},
	}
}

func TestMergeCanonicalSelectorWithGeneratedSourceAlias(t *testing.T) {
	base := selectorParam("Sort", "sort", "Creative", spec.SelectorPropertyOrderBy)
	base.TypeExpr = "string"
	base.Codec = &spec.Codec{Body: "OldCodec"}
	authored := selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)
	authored.TypeExpr = "[]string"
	authored.OutputTypeExpr = "string"
	authored.Codec = &spec.Codec{Body: "OrderByList"}
	got, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, []*spec.Parameter{authored})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "OrderBy", got[0].Name)
	require.Equal(t, "sort", got[0].Source.Name)
	require.Equal(t, "OrderByList", got[0].Codec.Body)
	require.Equal(t, "[]string", got[0].TypeExpr)
	require.Equal(t, "string", got[0].OutputTypeExpr)
	require.Equal(t, "OldCodec", base.Codec.Body, "package contract must remain detached")
}

func TestMergeSelectorAliasDoesNotSwallowOtherBindings(t *testing.T) {
	for _, tc := range []struct {
		name string
		base *spec.Parameter
	}{
		{"different source", selectorParam("Sort", "other", "creative", spec.SelectorPropertyOrderBy)},
		{"different view", selectorParam("Sort", "sort", "other", spec.SelectorPropertyOrderBy)},
		{"different property", selectorParam("Sort", "sort", "creative", spec.SelectorPropertyFields)},
		{"ordinary query", &spec.Parameter{Name: "Sort", Source: spec.BindSource{Kind: "query", Name: "sort"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (&Loader{}).mergeParams([]*spec.Parameter{tc.base}, []*spec.Parameter{selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)})
			require.NoError(t, err)
			require.Len(t, got, 2)
		})
	}
}

func TestMergeSelectorAliasRejectsCompetingAuthoredSelector(t *testing.T) {
	base := selectorParam("Sort", "sort", "creative", spec.SelectorPropertyOrderBy)
	authored := selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)
	got, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, []*spec.Parameter{authored, authored.Clone()})
	require.Nil(t, got)
	require.ErrorContains(t, err, "declared more than once")
}

func TestMergeSelectorAliasPreservesDistinctViewSources(t *testing.T) {
	base := []*spec.Parameter{
		selectorParam("ParentSort", "parent_sort", "parents", spec.SelectorPropertyOrderBy),
		selectorParam("ChildSort", "child_sort", "children", spec.SelectorPropertyOrderBy),
	}
	authored := []*spec.Parameter{
		selectorParam("OrderBy", "parent_sort", "parents", spec.SelectorPropertyOrderBy),
		selectorParam("OrderBy", "child_sort", "children", spec.SelectorPropertyOrderBy),
	}
	got, err := (&Loader{}).mergeParams(base, authored)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "parent_sort", got[0].Source.Name)
	require.Equal(t, "child_sort", got[1].Source.Name)
}

func TestMergeSelectorAliasPreservesOutputDirection(t *testing.T) {
	base := selectorParam("Sort", "sort", "creative", spec.SelectorPropertyOrderBy)
	base.EmitOutput = true
	authored := selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)
	got, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, []*spec.Parameter{authored})
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestMergeSelectorAliasRejectsExactAndAliasCompetition(t *testing.T) {
	for _, exactFirst := range []bool{false, true} {
		base := selectorParam("Sort", "sort", "creative", spec.SelectorPropertyOrderBy)
		alias := selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)
		authored := []*spec.Parameter{alias, base.Clone()}
		if exactFirst {
			authored[0], authored[1] = authored[1], authored[0]
		}
		_, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, authored)
		require.ErrorContains(t, err, "matches more than one authored selector")
	}
}

func TestCanonicalSideSelectorRetiresFormerCodecBridge(t *testing.T) {
	base := selectorParam("Sort", "sort", "creative", spec.SelectorPropertyOrderBy)
	base.TypeExpr = "[]string"
	base.OutputTypeExpr = "string"
	base.Codec = &spec.Codec{Body: "OrderByList"}
	authored := selectorParam("OrderBy", "sort", "creative", spec.SelectorPropertyOrderBy)
	authored.TypeExpr = "[]string"
	got, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, []*spec.Parameter{authored})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Nil(t, got[0].Codec)
	require.Empty(t, got[0].OutputTypeExpr)
	require.Equal(t, "[]string", got[0].TypeExpr)
	require.NotNil(t, base.Codec)
	require.Equal(t, "string", base.OutputTypeExpr)
}
