package view

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDetectCase(t *testing.T) {
	var testCases = []struct {
		names  []string
		expect string
	}{
		{
			names: []string{
				"NAME",
				"EMP_ID",
			},
			expect: "uu",
		},
		{
			names: []string{
				"eventTypeId",
				"event",
			},
			expect: "lc",
		},
	}

	for _, testCase := range testCases {
		actual := DetectCase(testCase.names...)
		assert.EqualValues(t, testCase.expect, actual)
	}

}
