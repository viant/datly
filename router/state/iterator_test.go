package state_test

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectorParamIt(t *testing.T) {
	testcases := []struct {
		description string
		expected    []state.Param
		value       string
	}{
		{
			description: "single value",
			value:       "10",
			expected:    []state.Param{{Value: "10"}},
		},
		{
			description: "view prefix",
			value:       "10",
			expected: []state.Param{
				{
					Value: "10",
				},
			},
		},
		{
			description: "multi param value",
			value:       "20,10",
			expected: []state.Param{
				{
					Value: "20",
				},
				{
					Value: "10",
				},
			},
		},
		{
			description: "multi param value, all with prefix",
			value:       "20,10",
			expected: []state.Param{
				{
					Value: "20",
				},
				{
					Value: "10",
				},
			},
		},
		{
			description: "value block",
			value:       "(SELECT * FROM public.abc),(SELECT * FROM public.ev where (1=1) AND (2=2))",
			expected: []state.Param{
				{
					Value: "SELECT * FROM public.abc",
				},
				{
					Value: "SELECT * FROM public.ev where (1=1) AND (2=2)",
				},
			},
		},
		{
			description: "empty string",
			value:       "",
			expected:    []state.Param{},
		},
		{
			description: "multiple empty values",
			value:       string(state.ValuesSeparator) + string(state.ValuesSeparator) + string(state.ValuesSeparator),
			expected:    []state.Param{{}, {}, {}},
		},
		{
			description: "expression blocks",
			value:       "()" + string(state.ValuesSeparator) + "()" + string(state.ValuesSeparator),
			expected:    []state.Param{{}, {}},
		},
	}

	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testcase := range testcases {
		it := state.NewParamIt(testcase.value)
		counter := 0
		for it.Has() {
			param, err := it.Next()
			assert.Equal(t, testcase.expected[counter], param, testcase.description)
			assert.Nil(t, err, testcase.description)

			counter++
		}
		assert.Equal(t, len(testcase.expected), counter, testcase.description)
	}
}
