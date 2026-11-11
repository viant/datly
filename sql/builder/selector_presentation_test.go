package builder

import (
	"context"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/sql/criteria"
)

func TestPresentedCriteriaKeepsAllowedTypedColumn(t *testing.T) {
	typed := criteria.Column{Expression: "(SUM(sample_seen_1_day))", Type: reflect.TypeFor[int](), TimeLayout: "source-layout"}
	columns := map[string]criteria.Column{"SampleSeen_1Day": typed}
	fields := []data.SelectorField{{GoName: "SampleSeen_1Day", PublicName: "sampleSeen1Day", Column: "sample_seen_1_day"}, {GoName: "Forbidden", PublicName: "forbidden", Column: "secret"}}
	require.NoError(t, addPresentedCriteriaAliases(columns, fields))
	require.Equal(t, typed, columns["sampleSeen1Day"])
	require.NotContains(t, columns, "forbidden")
}

func TestPresentedCriteriaRejectsAmbiguousColumnsWithoutMutation(t *testing.T) {
	tests := []map[string]criteria.Column{
		{"Metric": {Expression: "metric"}, "publicName": {Expression: "other"}},
		{"Metric": {Expression: "metric"}, "metric_column": {Expression: "other"}},
	}
	for _, columns := range tests {
		before := map[string]criteria.Column{}
		for key, value := range columns {
			before[key] = value
		}
		err := addPresentedCriteriaAliases(columns, []data.SelectorField{{GoName: "Metric", PublicName: "publicName", Column: "metric_column"}})
		require.ErrorContains(t, err, "ambiguous criteria presentation")
		require.Equal(t, before, columns)
	}
}

func TestSelectorPresentationBuilderOptionDetachesMetadata(t *testing.T) {
	fields := []data.SelectorField{{GoName: "Metric", PublicName: "metric", Index: []int{1}}}
	option := WithBuilderSelectorPresentation(fields)
	fields[0].PublicName = "changed"
	fields[0].Index[0] = 9
	options := newBuilderOptions(option)
	require.Equal(t, "metric", options.selectorPresentation[0].PublicName)
	require.Equal(t, []int{1}, options.selectorPresentation[0].Index)
}

func TestPresentedOrderingPreservesDefaultsAndPositions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector *xstate.Selector
		want     string
	}{
		{"authored default", nil, "SELECT id, name FROM users ORDER BY name ASC"},
		{"numeric position", &xstate.Selector{OrderBy: "1 DESC"}, "SELECT id, name FROM users ORDER BY 1 DESC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT id, name FROM users"), WithBuilderInput(reflect.ValueOf(struct{}{})), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, DefaultOrder: "name ASC", Orderable: []spec.FieldPath{"id", "name"}}), WithBuilderSelector(tc.selector), WithBuilderSelectorPresentation([]data.SelectorField{{PublicName: "name", GoName: "id"}, {PublicName: "1", GoName: "name"}}))
			require.NoError(t, err)
			require.Equal(t, tc.want, query.SQL)
		})
	}
}
