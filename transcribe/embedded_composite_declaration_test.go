package transcribe

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	dsql "github.com/viant/datly/sql"
)

func TestCompileEmbeddedCompositeDeclaration(t *testing.T) {
	const directive = `#define($_ = $CurrentPatch<?>(view/CurrentPatch).Cardinality('Many') /* SELECT c.*,row_lock(c,'CI_CUSTOM_SEGMENT s','s.ID'),set_limit(c,0) FROM (${embed:sql/current.sql}) c */)`
	const current = `SELECT s.ID, s.ADVERTISER_ID FROM CI_CUSTOM_SEGMENT s
WHERE s.ADVERTISER_ID=$AdvertiserID AND $criteria.CompositeIn("s",$PatchKeys)
AND EXISTS (
    SELECT 1 FROM CI_ADVERTISER adv
    WHERE adv.ID=s.ADVERTISER_ID AND adv.ARCHIVED=0
    ${predicate.Builder().CombineOr($predicate.FilterGroup(0,"AND")).Build("AND")}
)`
	for _, tc := range []struct {
		name, sql, diagnostic string
	}{
		{name: "advertiser-scoped current rows", sql: current},
		{name: "invalid arity", sql: `SELECT s.ID FROM CI_CUSTOM_SEGMENT s WHERE $criteria.CompositeIn("s")`, diagnostic: "two arguments"},
		{name: "chained template", sql: `SELECT s.ID FROM CI_CUSTOM_SEGMENT s WHERE $criteria.CompositeIn("s",$PatchKeys).Other()`, diagnostic: "no chained access"},
		{name: "projection", sql: `SELECT $criteria.CompositeIn("s",$PatchKeys) AS ID FROM CI_CUSTOM_SEGMENT s`, diagnostic: "predicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resources := resource.New()
			require.NoError(t, resources.Register("", fstest.MapFS{"sql/current.sql": &fstest.MapFile{Data: []byte(tc.sql)}}))
			source := &Source{Scope: "example.com/analysis", Name: "Segments", Resources: resources,
				Text: `#setting($_ = $route('/segments', 'PATCH'))
#define($_ = $AdvertiserID<int>(query/advertiserId).Required())
#define($_ = $PatchKeys<[]int>(query/patchKeys))
` + directive + "\nSELECT ID, ADVERTISER_ID FROM CI_CUSTOM_SEGMENT"}
			result, err := NewCompiler().Compile(context.Background(), source)
			if tc.diagnostic != "" {
				require.ErrorContains(t, err, "analyze declaration SQL for parameter CurrentPatch")
				require.ErrorContains(t, err, tc.diagnostic)
				return
			}
			require.NoError(t, err)
			for _, view := range result.Component.Views {
				if view.Name == "CurrentPatch" {
					resolved := view.Source.Clone()
					require.NoError(t, dsql.ResolveSource(view.Name, resolved, resources))
					require.Contains(t, resolved.SQL, `$criteria.CompositeIn("s",$PatchKeys)`)
					require.Contains(t, resolved.SQL, `s.ADVERTISER_ID=$AdvertiserID`)
					require.Contains(t, resolved.SQL, `${predicate.Builder().CombineOr($predicate.FilterGroup(0,"AND")).Build("AND")}`)
					require.NotContains(t, resolved.SQL, "__datly_analysis_predicate_")
					return
				}
			}
			t.Fatal("missing CurrentPatch view")
		})
	}
}
