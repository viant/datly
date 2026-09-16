package sql

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestForecastingProjection(t *testing.T) {
	raw, err := os.ReadFile("testdata/steward_forecasting.sql")
	require.NoError(t, err)
	// Identical to Steward's embedded SQL; only substitute the runtime predicate.
	authored := strings.ReplaceAll(string(raw), `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`, "WHERE DATE(v.hstamp) = ? AND v.channelV2 = ?")
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on, AllowNulls: &on}}
	for _, wrapped := range []bool{false, true} {
		input := strings.TrimSpace(authored)
		if !wrapped {
			input = strings.TrimSpace(input[1 : len(input)-1])
		}
		for _, tc := range []struct {
			name   string
			fields []string
			groups int
		}{
			{"full", nil, 42},
			{"retain_iab", []string{"event_date", "site_id", "iab_cats", "avails"}, 2},
			{"drop_iab", []string{"event_date", "avails"}, 1},
			{"measures", []string{"iab_cats", "avails"}, 0},
		} {
			t.Run(tc.name+map[bool]string{false: "_bare", true: "_wrapped"}[wrapped], func(t *testing.T) {
				result, err := ApplySelectorProjection(input, tc.fields, view)
				require.NoError(t, err)
				text := strings.TrimSpace(result)
				if strings.HasPrefix(text, "(") {
					text = strings.TrimSpace(text[1 : len(text)-1])
				}
				q, err := sqlparser.ParseQuery(text)
				require.NoError(t, err, result)
				require.Len(t, q.GroupBy, tc.groups)
				require.Contains(t, result, "WHERE DATE(v.hstamp) = ? AND v.channelV2 = ?")
				if tc.fields == nil {
					require.Equal(t, unwrapProjectionSQL(input), result, "full projection must preserve SQL except its outer enclosure")
				} else {
					require.Len(t, q.List, len(tc.fields))
					var names []string
					for _, item := range q.List {
						names = append(names, sqlparser.NewColumn(item).Identity())
					}
					require.Equal(t, tc.fields, names)
					for i, item := range q.GroupBy {
						require.Equal(t, strconv.Itoa(i+1), sqlparser.Stringify(item.Expr))
					}
				}
				if tc.name != "drop_iab" {
					require.Contains(t, result, "STRING_AGG(DISTINCT IAB[SAFE_OFFSET(0)], ', ' LIMIT 20)")
				} else {
					require.NotContains(t, result, "STRING_AGG")
				}
			})
		}
	}
}

func TestAggregateSubscriptClassification(t *testing.T) {
	for _, expression := range []string{"ARRAY_AGG(id)[OFFSET(0)]", "IFNULL(ARRAY_AGG(id)[OFFSET(0)], 0)"} {
		q, err := sqlparser.ParseQuery("SELECT " + expression + " AS result FROM records")
		require.NoError(t, err)
		require.True(t, isAggregateSelectItem(q.List[0]))
		require.Empty(t, groupedProjectionGroupBy(q.List))
	}
	q, err := sqlparser.ParseQuery("SELECT arr[OFFSET(0)] AS result FROM records")
	require.NoError(t, err)
	require.False(t, isAggregateSelectItem(q.List[0]))
}

func TestProjectionWholeQueryEnclosure(t *testing.T) {
	for _, input := range []string{"(SELECT id FROM records)", "( (SELECT id FROM records) )"} {
		result, err := ApplySelectorProjection(input, []string{"id"}, &data.View{})
		require.NoError(t, err)
		require.Equal(t, "SELECT id FROM records", result)
		found, complete, err := (SelectorProjection{SQL: input}).HasOutput("id")
		require.NoError(t, err)
		require.True(t, found && complete)
	}
	for _, input := range []string{"(SELECT id FROM a) UNION ALL (SELECT id FROM b)", "SELECT * FROM (SELECT id FROM a) t", "(SELECT id FROM a) trailing", "(SELECT id FROM a"} {
		require.Equal(t, input, unwrapProjectionSQL(input))
	}
	_, err := ApplySelectorProjection("SELECT IAB[] AS bad FROM records", nil, &data.View{})
	require.Error(t, err)
	require.NotNil(t, errors.Unwrap(err), "retain the underlying parser failure")
}
