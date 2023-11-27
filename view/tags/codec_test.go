package tags

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestTag_updateCodec(t *testing.T) {
	var testCases = []struct {
		description string
		tag         reflect.StructTag
		expect      *Codec
		expectTag   string
	}{

		{
			description: "basic codec",
			tag:         `codec:"c1,A1,A2"`,
			expect:      &Codec{Name: "c1", Arguments: []string{"A1", "A2"}},
		},
		{
			description: "basic codec",
			tag:         `codec:"c1,A1,A2"`,
			expect:      &Codec{Name: "c1", Arguments: []string{"A1", "A2"}},
		},
	}

	for _, testCase := range testCases {
		actual, err := Parse(testCase.tag, &embedFS, CodecTag)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual.Codec, testCase.description)
		expectTag := testCase.expectTag
		if expectTag == "" {
			expectTag = testCase.tag.Get(CodecTag)
		}
		assert.EqualValues(t, expectTag, string(actual.Codec.Tag().Values), testCase.description)
	}
}
