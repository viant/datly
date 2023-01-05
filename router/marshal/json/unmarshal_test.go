package json

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/router/marshal"
	"reflect"
	"testing"
)

func TestMarshaller_Unmarshal(t *testing.T) {
	testCases := []struct {
		description  string
		data         string
		into         func() interface{}
		expect       string
		expectError  bool
		stringsEqual bool
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
					Has  *FooHas `presenceIndex:"true"`
				}

				return &[]*Foo{}
			},
			expect: `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
		},
		{
			description: "setting has",
			data:        `[{"ID": 1, "Has": {"ID": true, "Name": "true"}}, {"Name": "Boo"}]`,
			expect:      `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `presenceIndex:"true"`
				}

				return &[]*Foo{}
			},
		},
		{
			description: "setting has",
			data:        `[{"ID": 1, "Has": {"ID": true, "Name": "true"}}, {"Name": "Boo"}]`,
			expect:      `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `presenceIndex:"true"`
				}

				return &[]*Foo{}
			},
		},
		{
			description: "multi nesting",
			data: `[
	{
		"Size": 1,
		"Foos":[
			{"WrapperID": 1, "WrapperName": "wrapper - 1", "Foos": [{"ID": 10, "Name": "foo - 10"}]},
			{"WrapperID": 2, "WrapperName": "wrapper - 2", "Foos": [{"ID": 20, "Name": "foo - 20"}]}
		]
	}
]`,
			expect: `[{"Foos":[{"WrapperID":1,"WrapperName":"wrapper - 1","Foos":[{"ID":10,"Name":"foo - 10","Has":{"ID":true,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":true}},{"WrapperID":2,"WrapperName":"wrapper - 2","Foos":[{"ID":20,"Name":"foo - 20","Has":{"ID":true,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":true}}],"Size":1}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `presenceIndex:"true"`
				}

				type WrapperHas struct {
					WrapperID   bool
					WrapperName bool
				}

				type FooWrapper struct {
					WrapperID   int
					WrapperName string
					Foos        []*Foo
					Has         *WrapperHas `presenceIndex:"true"`
				}

				type Data struct {
					Foos []*FooWrapper
					Size int
				}

				return &[]*Data{}
			},
		},
		{
			description: "multi nesting",
			data: `[
	{
		"Size": 1,
		"Foos":[
			{"WrapperName": "wrapper - 1", "Foos": [{"ID": 10}]},
			{"WrapperID": 2, "Foos": [{"Name": "foo - 20"}]}
		]
	}
]`,
			expect: `[{"Foos":[{"WrapperID":0,"WrapperName":"wrapper - 1","Foos":[{"ID":10,"Name":"","Has":{"ID":true,"Name":false}}],"Has":{"WrapperID":false,"WrapperName":true}},{"WrapperID":2,"WrapperName":"","Foos":[{"ID":0,"Name":"foo - 20","Has":{"ID":false,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":false}}],"Size":1}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `presenceIndex:"true"`
				}

				type WrapperHas struct {
					WrapperID   bool
					WrapperName bool
				}

				type FooWrapper struct {
					WrapperID   int
					WrapperName string
					Foos        []*Foo
					Has         *WrapperHas `presenceIndex:"true"`
				}

				type Data struct {
					Foos []*FooWrapper
					Size int
				}

				return &[]*Data{}
			},
		},
		{
			description: "primitive slice",
			data:        `[1,2,3,4,5]`,
			expect:      `[1,2,3,4,5]`,
			into: func() interface{} {
				return new([]int)
			},
		},
		{
			description:  "nulls",
			data:         `{"ID":null,"Name":null}`,
			stringsEqual: true,
			into: func() interface{} {
				type Foo struct {
					ID   *int
					Name *string
				}

				return &Foo{}
			},
		},
		{
			description:  "empty presence index",
			data:         `{}`,
			expect:       `{"Has":{"ID":false,"Name":false}}`,
			stringsEqual: true,
			into: func() interface{} {
				type FooHasIndex struct {
					ID   bool
					Name bool
				}
				type Foo struct {
					ID   *int         `json:",omitempty"`
					Name *string      `json:",omitempty"`
					Has  *FooHasIndex `presenceIndex:"true"`
				}

				return &Foo{}
			},
		},
	}

	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases {
		fmt.Printf("Running testcase nr#%v\n", i)
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

		if !testCase.stringsEqual {
			assertly.AssertValues(t, expect, dest, testCase.description)
		} else {
			bytes, _ := json.Marshal(dest)
			assert.Equal(t, expect, string(bytes), testCase.description)
		}
	}
}
