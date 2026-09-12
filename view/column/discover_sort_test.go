package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/sink"
)

func names(columns []sink.Column) []string {
	var result []string
	for _, column := range columns {
		result = append(result, column.Name)
	}
	return result
}

// config.Columns runs an information_schema query with no ORDER BY, so the
// driver may hand back columns in any order. Without a sort, that order leaks
// into generated struct field order and churns on every regeneration.
func TestSortByPosition(t *testing.T) {
	testCases := []struct {
		description string
		columns     []sink.Column
		expect      []string
	}{
		{
			description: "alphabetical metadata order is restored to table order",
			columns: []sink.Column{
				{Name: "CREATED", Position: 5},
				{Name: "CREATED_USER", Position: 7},
				{Name: "FEE_DOMAIN", Position: 4},
				{Name: "FEE_TYPE_ID", Position: 3},
				{Name: "ID", Position: 1},
				{Name: "NAME", Position: 2},
				{Name: "UPDATED", Position: 6},
				{Name: "UPDATED_USER", Position: 8},
			},
			expect: []string{"ID", "NAME", "FEE_TYPE_ID", "FEE_DOMAIN", "CREATED", "UPDATED", "CREATED_USER", "UPDATED_USER"},
		},
		{
			description: "already ordered stays ordered",
			columns: []sink.Column{
				{Name: "ID", Position: 1},
				{Name: "NAME", Position: 2},
			},
			expect: []string{"ID", "NAME"},
		},
		{
			description: "result-set inferred columns carry no position, so projection order is preserved",
			columns: []sink.Column{
				{Name: "TOTAL_SPEND"},
				{Name: "AGENCY_ID"},
			},
			expect: []string{"TOTAL_SPEND", "AGENCY_ID"},
		},
		{
			description: "empty input is a no-op",
			columns:     []sink.Column{},
			expect:      nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			sortByPosition(testCase.columns)
			assert.Equal(t, testCase.expect, names(testCase.columns))
		})
	}
}

// Sorting must be idempotent, otherwise repeated regeneration would still churn.
func TestSortByPosition_Idempotent(t *testing.T) {
	columns := []sink.Column{
		{Name: "UPDATED", Position: 6},
		{Name: "ID", Position: 1},
		{Name: "NAME", Position: 2},
	}
	sortByPosition(columns)
	first := names(columns)
	sortByPosition(columns)
	assert.Equal(t, first, names(columns))
}
