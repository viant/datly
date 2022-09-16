package csv

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"reflect"
	"testing"
)

func TestCsv_Unmarshal(t *testing.T) {
	return
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	testCases := []struct {
		description string
		rType       reflect.Type
		input       string
		expected    string
	}{
		{
			description: "basic",
			input: `ID,Name,Price
1,"foo",125.5`,
			rType:    reflect.TypeOf(Foo{}),
			expected: `{"ID": 1, "Name": "foo", "Price": 125.5}`,
		},
	}

	for _, testCase := range testCases {
		marshaller, err := NewMarshaller(testCase.rType)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		dest := reflect.New(reflect.SliceOf(testCase.rType)).Interface()
		if !assert.Nil(t, marshaller.Unmarshal([]byte(testCase.input), dest), testCase.description) {
			continue
		}

		assertly.AssertValues(t, testCase.expected, dest)
	}
}
