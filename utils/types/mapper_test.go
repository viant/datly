package types

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"reflect"
	"testing"
)

func TestNewMapper(t *testing.T) {
	testCases := []struct {
		description   string
		src           func() interface{}
		dst           func() interface{}
		expectedError bool
		expectedValue interface{}
	}{
		{
			src: func() interface{} {
				type Foo struct {
					ID    int
					Name  string
					Price float64
				}

				return &Foo{
					ID:    10,
					Name:  "Foo",
					Price: 125.50,
				}
			},
			dst: func() interface{} {
				type FooValue struct {
					Name  string
					Price float64
				}

				return &FooValue{}
			},
			expectedValue: `{"Name": "Foo", "Price": 125.5}`,
		},
		{
			description: "with has marker",
			src: func() interface{} {
				type FooHas struct {
					ID    bool
					Name  bool
					Price bool
				}
				type Foo struct {
					ID    int
					Name  string
					Price float64
					Has   *FooHas `setMarker:"true"`
				}

				return &Foo{
					ID:    10,
					Name:  "Foo",
					Price: 125.50,
					Has: &FooHas{
						ID:    true,
						Name:  true,
						Price: true,
					},
				}
			},
			dst: func() interface{} {
				type FooValueHas struct {
					Name  bool
					Price bool
				}
				type FooValue struct {
					Name  string
					Price float64
					Has   *FooValueHas `setMarker:"true"`
				}

				return &FooValue{}
			},
			expectedValue: `{"Name": "Foo", "Price": 125.5, "Has": {"Name": true, "Price": true}}`,
		},
	}

	for _, testCase := range testCases {
		src := testCase.src()
		dst := testCase.dst()

		mapper, err := NewMapper(reflect.TypeOf(src), reflect.TypeOf(dst))
		if testCase.expectedError {
			if assert.Nil(t, err, testCase.description) {
				continue
			}
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		result, err := mapper.Map(src)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		assertly.AssertValues(t, testCase.expectedValue, result, testCase.description)
	}
}
