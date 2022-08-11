package ast

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestExtractHints(t *testing.T) {

	var testCases = []struct {
		description string
		text        string
		expect      string
	}{
		{
			description: "basic hint",
			text: `
/* this is hint */
ABC
`,
			expect: "/* this is hint */",
		},
	}

	for _, testCase := range testCases {
		actual := ExtractHint(testCase.text)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}

func TestExtractParameterHints(t *testing.T) {
	var testCases = []struct {
		description string
		text        string
		expect      ParameterHints
	}{
		{
			description: "expr with hints",
			text: `
$abc
$zyx 
/* this is 1st hint */

$yyy

$xx /* this is 2nd hint */


/* this is non matching hint */

`,
			expect: ParameterHints{
				{
					Parameter: "zyx",
					Hint:      "/* this is 1st hint */",
				},
				{
					Parameter: "xx",
					Hint:      "/* this is 2nd hint */",
				},
			},
		},
	}

	for _, testCase := range testCases {
		actual := ExtractParameterHints(testCase.text)
		toolbox.Dump(actual)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}

func TestUnmarshalHint(t *testing.T) {
	type setting struct {
		Enabled bool
		MaxConn int
	}
	var testCases = []struct {
		description string
		hint        string
		dest        setting
		remainder   string
		expect      setting
	}{
		{
			description: "basic json hint",
			hint:        `/* {"MaxConn":2000} */`,
			expect: setting{
				MaxConn: 2000,
			},
			remainder: "",
		},
		{
			description: "hinst with SQL",
			hint:        `/* {"MaxConn":2001} SELECT * FROM foo */`,
			expect: setting{
				MaxConn: 2001,
			},
			remainder: "SELECT * FROM foo",
		},
	}

	for _, testCase := range testCases {
		actual, err := UnmarshalHint(testCase.hint, &testCase.dest)
		assert.Nil(t, err, testCase.description)
		assert.EqualValues(t, testCase.expect, testCase.dest, testCase.description)
		assert.EqualValues(t, testCase.remainder, actual, testCase.description)

	}
}
