package tags

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestTag_updatePredicate(t *testing.T) {
	var testCases = []struct {
		description string
		tag         reflect.StructTag
		expect      *Predicate
		expectTag   string
	}{

		{
			description: "basic predicate",
			tag:         `predicate:"p1,group=1,A1,A2"`,
			expect:      &Predicate{Name: "p1", Group: 1, Arguments: []string{"A1", "A2"}},
		},
	}

	for _, testCase := range testCases {
		actual, err := Parse(testCase.tag, &embedFS, PredicateTag)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual.Predicate, testCase.description)
		expectTag := testCase.expectTag
		if expectTag == "" {
			expectTag = testCase.tag.Get(PredicateTag)
		}
		assert.EqualValues(t, expectTag, string(actual.Predicate.Tag().Values), testCase.description)
	}
}
