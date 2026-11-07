package sql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"testing"
)

func seedSourceProjection(source string) SelectorProjection {
	return SelectorProjection{SQL: source, View: &data.View{SelectorFields: []data.SelectorField{{GoName: "TaxonomyID", PublicName: "taxonomyId", Column: "ID", ColumnAliases: []string{"TAXONOMY_ID"}, VerifiedColumnAliases: []string{"TAXONOMY_ID"}, SourceAlias: "ID", SourceAliasColumns: []string{"ID", "TAXONOMY_ID"}}, {GoName: "TaxonomyName", PublicName: "taxonomyName", Column: "NAME", ColumnAliases: []string{"TAXONOMY_NAME"}, VerifiedColumnAliases: []string{"TAXONOMY_NAME"}}}}}
}
func TestDeclaredSourceProjectionFetchesOnlyExistingResult(t *testing.T) {
	p := seedSourceProjection("SELECT t.ID AS TAXONOMY_ID,t.NAME AS TAXONOMY_NAME FROM taxonomy t")
	for _, name := range []string{"id", "ID"} {
		cols, err := p.Columns([]string{name})
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "TAXONOMY_ID", cols[0].OutputName())
		require.False(t, cols[0].Matches(name), "source-only selection must not authorize ordering")
		yes, err := p.SourceOnlySelection(name)
		require.NoError(t, err)
		require.True(t, yes)
		q, err := p.Prepare([]string{name})
		require.NoError(t, err)
		require.Equal(t, "SELECT t.ID AS TAXONOMY_ID FROM taxonomy t", q.Render(q.Source))
	}
	cols, err := p.Columns([]string{"id", "taxonomyId"})
	require.NoError(t, err)
	require.Len(t, cols, 1)
	yes, err := p.SourceOnlySelection("taxonomyId")
	require.NoError(t, err)
	require.False(t, yes)
	for _, name := range []string{"missing", "t.ID"} {
		_, err := p.Columns([]string{name})
		require.Error(t, err)
	}
}
func TestDeclaredSourceProjectionRetainsMembershipGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*SelectorProjection)
	}{
		{"no provenance", func(p *SelectorProjection) { p.View.SelectorFields[0].SourceAlias = "" }},
		{"unverified target", func(p *SelectorProjection) { p.View.SelectorFields[0].SourceAliasColumns = []string{"ID"} }},
		{"missing output", func(p *SelectorProjection) { p.SQL = "SELECT t.NAME AS TAXONOMY_NAME FROM taxonomy t" }},
		{"duplicate source claims", func(p *SelectorProjection) { p.View.SelectorFields[1].SourceAlias = "ID" }},
		{"competing secondary results", func(p *SelectorProjection) {
			p.SQL = "SELECT ID AS TAXONOMY_ID,NAME AS OTHER_ID FROM taxonomy"
			f := &p.View.SelectorFields[0]
			f.ColumnAliases = append(f.ColumnAliases, "OTHER_ID")
			f.VerifiedColumnAliases = append(f.VerifiedColumnAliases, "OTHER_ID")
			f.SourceAliasColumns = append(f.SourceAliasColumns, "OTHER_ID")
		}},
		{"unverified destination", func(p *SelectorProjection) { p.View.SelectorFields[0].VerifiedColumnAliases = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := seedSourceProjection("SELECT t.ID AS TAXONOMY_ID,t.NAME AS TAXONOMY_NAME FROM taxonomy t")
			tc.mutate(&p)
			_, err := p.Columns([]string{"id"})
			require.Error(t, err)
			yes, err := p.SourceOnlySelection("id")
			require.NoError(t, err)
			require.False(t, yes)
		})
	}
	p := seedSourceProjection("SELECT t.ID AS TAXONOMY_ID,t.NAME AS TAXONOMY_NAME FROM taxonomy t")
	p.View.Spec.Columns = []*spec.Column{{Name: "id", Source: "TAXONOMY_ID"}}
	cols, err := p.Columns([]string{"id"})
	require.NoError(t, err)
	require.Len(t, cols, 1)
	yes, err := p.SourceOnlySelection("id")
	require.NoError(t, err)
	require.False(t, yes, "explicit canonical mappings retain existing public output semantics")
}
