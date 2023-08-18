package predicate

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"testing"
)

func TestEvaluator_Expand(t *testing.T) {

	var testCases = []struct {
		description string
		template    *predicate.Template
		value       interface{}
		args        []string
		expected    *codec.Criteria
	}{
		{
			description: "single value expand - no args",
			template: &predicate.Template{
				Name:   "foo",
				Source: "NAME = $FilterValue",
				Args:   []*predicate.NamedArgument{},
			},
			value: "Adam",
			expected: &codec.Criteria{
				Query: "NAME = ?",
				Args:  []interface{}{"Adam"},
			},
		},
		{
			description: "single value expand - single args",
			template: &predicate.Template{
				Name:   "bar",
				Source: "$Column = $FilterValue",
				Args: []*predicate.NamedArgument{
					{
						Name:     "Column",
						Position: 0,
					},
				},
			},
			args:  []string{"active"},
			value: true,
			expected: &codec.Criteria{
				Query: "active = ?",
				Args:  []interface{}{true},
			},
		},
		{
			description: "multi value expand",
			template: &predicate.Template{
				Name:   "dummy",
				Source: "$Column IN($FilterValue)",
				Args: []*predicate.NamedArgument{
					{
						Name:     "Column",
						Position: 0,
					},
				},
			},
			args:  []string{"id"},
			value: []int{1, 10, 100},
			expected: &codec.Criteria{
				Query: "id IN(?,?,?)",
				Args:  []interface{}{1, 10, 100},
			},
		},
	}

	for _, testCase := range testCases {
		evaluator := NewEvaluator(testCase.template, testCase.args...)
		actual, err := evaluator.Expand(testCase.value)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expected, actual)

	}
}
