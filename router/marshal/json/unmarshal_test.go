package json

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/router/marshal"
	"reflect"
	"testing"
)

func TestMarshaller_Unmarshal(t *testing.T) {
	testCases := []struct {
		description string
		data        string
		into        func() interface{}
		expect      string
		expectError bool
	}{
		{
			description: "basic struct",
			data:        `{"Name": "Foo", "ID": 1}`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &Foo{}
			},
		},
		{
			description: "basic slice",
			data:        `[{"Name": "Foo", "ID": 1},{"Name": "Boo", "ID": 2}]`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &[]*Foo{}
			},
		},
		{
			description: "empty slice",
			data:        `[]`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &[]*Foo{}
			},
		},
		{
			description: "has",
			data:        `[{"ID": 1}, {"Name": "Boo"}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `jsonIndex:"true"`
				}

				return &[]*Foo{}
			},
			expect: `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
		},
		{
			description: "setting has",
			data:        `[{"ID": 1, "Has": {"ID": true, "Name": "true"}}, {"Name": "Boo"}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `jsonIndex:"true"`
				}

				return &[]*Foo{}
			},
			expectError: true,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		dest := testCase.into()
		marshaller, err := New(reflect.TypeOf(dest), marshal.Default{})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		marshalErr := marshaller.Unmarshal([]byte(testCase.data), dest)

		if (!testCase.expectError && !assert.Nil(t, err, testCase.description)) || (testCase.expectError && assert.NotNil(t, marshalErr, testCase.description)) {
			continue
		}

		expect := testCase.expect
		if testCase.expect == "" {
			expect = testCase.data
		}

		assertly.AssertValues(t, expect, dest, testCase.description)
	}
}
