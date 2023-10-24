package tags

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestTag_updateParameter(t *testing.T) {
	var testCases = []struct {
		description string
		tag         reflect.StructTag
		expect      *Parameter
		expectTag   string
	}{

		{
			description: "basic Parameter",
			tag:         `parameter:"p1,kind=query,in=qp1"`,
			expect:      &Parameter{Name: "p1", Kind: "query", In: "qp1"},
		},
	}

	for _, testCase := range testCases {
		actual, err := Parse(testCase.tag, &embedFS, ParameterTag)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual.Parameter, testCase.description)
		expectTag := testCase.expectTag
		if expectTag == "" {
			expectTag = testCase.tag.Get(ParameterTag)
		}
		assert.EqualValues(t, expectTag, string(actual.Parameter.Tag().Values), testCase.description)
	}
}
