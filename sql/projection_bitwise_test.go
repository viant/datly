package sql

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestForecastingProjectionRejectsIncompleteExpressions(t *testing.T) {
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on, AllowNulls: &on}}
	for _, input := range []string{
		"SELECT country, events & AS mask, SUM(avails) AS avails FROM records GROUP BY country, mask",
		"SELECT country, events << AS mask, SUM(avails) AS avails FROM records GROUP BY country, mask",
		"SELECT country, a || b AS label, SUM(avails) AS avails FROM records GROUP BY country, label",
		"SELECT country, SUM(avails) AS avails FROM records WHERE events & GROUP BY country",
		"SELECT country, SUM(avails) AS avails FROM records GROUP BY country HAVING SUM(avails) |",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ApplySelectorProjection(input, []string{"country", "avails"}, view)
			require.Error(t, err, "validate the entire source before pruning any projection")
		})
	}
}

func TestForecastingBitwiseProjection(t *testing.T) {
	raw, err := os.ReadFile("testdata/steward_forecasting.sql")
	require.NoError(t, err)
	const macro = `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`
	require.Contains(t, string(raw), macro)
	const base = "DATE(v.hstamp) >= DATE(?) AND DATE(v.hstamp) <= DATE(?) AND v.channelV2 IN (?) AND v.country IN (?)"
	const mask = "BIT_COUNT(s.events & (1 << (v.seq))) > 0"
	filter := func(table, exists, values string) string {
		return fmt.Sprintf("(%s(SELECT 1 FROM `adelphic-forecasting.ci_event.%s` s WHERE (DATE(s.hstamp) BETWEEN DATE(?) AND DATE(?)) AND value IN (%s) AND v.batch_id = s.batch_id AND %s))", exists, table, values, mask)
	}
	iris := filter("audience_event_data_iris_segment_id", "EXISTS", "?")
	siteInclude := filter("audience_event_data_site_list_ids", "EXISTS", "?")
	siteExclude := filter("audience_event_data_site_list_ids", "NOT EXISTS", "?, ?, ?")
	viant := filter("audience_event_data_viant_tax", "EXISTS", "?")
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on, AllowNulls: &on}}
	fields := []string{"country", "region", "avails", "bids"}
	for _, tc := range []struct{ name, predicate string }{
		{"iris", iris},
		{"site_include", siteInclude},
		{"site_exclude", siteExclude},
		{"viant", viant},
		{"combined", strings.Join([]string{"(((v.country = ? AND v.region = ?)))", iris, siteInclude, siteExclude, viant}, " AND ")},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", tc.name, wrapped), func(t *testing.T) {
				where := "WHERE " + base + " AND " + tc.predicate
				input := strings.TrimSpace(strings.ReplaceAll(string(raw), macro, where))
				if !wrapped {
					input = unwrapProjectionSQL(input)
				}
				full, err := ApplySelectorProjection(input, nil, view)
				require.NoError(t, err)
				require.Equal(t, unwrapProjectionSQL(input), full)
				result, err := ApplySelectorProjection(input, fields, view)
				require.NoError(t, err)
				require.Contains(t, result, where, "preserve the complete filter, including exclusion predicates")
				require.Equal(t, strings.Count(input, "?"), strings.Count(result, "?"))
				require.Equal(t, strings.Count(input, mask), strings.Count(result, mask))
				q, err := sqlparser.ParseQuery(unwrapProjectionSQL(result))
				require.NoError(t, err)
				var names []string
				for _, item := range q.List {
					names = append(names, sqlparser.NewColumn(item).Identity())
				}
				require.Equal(t, fields, names)
				require.Len(t, q.GroupBy, 2)
				require.Equal(t, "1", sqlparser.Stringify(q.GroupBy[0].Expr))
				require.Equal(t, "2", sqlparser.Stringify(q.GroupBy[1].Expr))

				invalid := strings.ReplaceAll(input, mask, "BIT_COUNT(s.events & (1 <<)) > 0")
				_, err = ApplySelectorProjection(invalid, fields, view)
				require.Error(t, err, "projection rewriting must retain parser validation")
			})
		}
	}
}
