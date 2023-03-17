package formatter

import (
	"github.com/stretchr/testify/assert"
	"strings"
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
		{
			names: []string{
				"EVENT",
			},
			expect: "uu",
		},
		{
			names: []string{
				"event",
			},
			expect: "lu",
		},
		{
			names: []string{
				"ID",
			},
			expect: "uu",
		},
	}

	for _, testCase := range testCases {
		actual := DetectCase(testCase.names...)
		assert.EqualValues(t, testCase.expect, actual, testCase.expect+" "+strings.Join(testCase.names, ","))
	}

}
