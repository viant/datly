package compile

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"testing"
)

func TestDeclarationUsesReaderTemplateAnalysis(t *testing.T) {
	source := `SELECT r.id FROM records r WHERE 1=1
${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")}
#if($Has.CampaignIds)
AND EXISTS (SELECT 1 FROM mappings m WHERE m.record_id=r.id)
#end
ORDER BY r.id LIMIT 51`
	a, err := dql.AnalyzeDeclarationSQL(source, DeclarationAnalysisSource)
	require.NoError(t, err)
	require.Equal(t, source, a.SQL, "executable source must retain predicates and conditions")
	require.True(t, a.ProjectionComplete)
	require.Equal(t, "id", a.Projection[0].Source)
	view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "scope", Source: &spec.ViewSource{SQL: source}}, SQL: source})
	require.NoError(t, err)
	require.Contains(t, view.Source.SQL, "#if($Has.CampaignIds)")
	require.Contains(t, view.Source.SQL, `${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")}`)
	wrapped := "SELECT scoped.* FROM (" + source + ") scoped"
	view, err = NewReader().Compile(ReadInput{View: &spec.View{Name: "scope", Source: &spec.ViewSource{SQL: wrapped}}, SQL: wrapped})
	require.NoError(t, err)
	require.Contains(t, view.Source.SQL, "#if($Has.CampaignIds)")
	require.Contains(t, view.Source.SQL, `${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")}`)
	require.NotContains(t, view.Source.SQL, "__datly_analysis_predicate_")
	_, err = dql.AnalyzeDeclarationSQL("SELECT id FROM records\n#if($Has.X)\nAND", DeclarationAnalysisSource)
	require.Error(t, err)
}
