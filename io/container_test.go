package io

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"testing"
)




func TestContainer_Add(t *testing.T) {

	type Foo struct {
		ID int
	}

	var fooArray1 = []Foo{}
	var fooArray2 = []*Foo{}

	var testCases = []struct {
		description string
		target      interface{}
		repeat      int
		expect      interface{}
		modifier    func(item interface{}, i int)
	}{

		{
			description: "foo array",
			target:      &fooArray1,
			repeat:      2,
			modifier: func(item interface{}, i int) {
				item.(*Foo).ID = i
			},
			expect: `[{"ID":0},{"ID":1}]`,
		},
		{
			description: "*foo array",
			target:      &fooArray2,
			repeat:      2,
			modifier: func(item interface{}, i int) {
				item.(*Foo).ID = i
			},
			expect: `[{"ID":0},{"ID":1}]`,
		},
		{
			description: "foo",
			target:      &Foo{},
			repeat:      2,
			modifier: func(item interface{}, i int) {
				item.(*Foo).ID = i + 10
			},
			expect: `{"ID":10}`,
		},
	}

	for _, testCase := range testCases {
		aContainer, err := NewStructContainer(testCase.target)
		assert.Nil(t, err, testCase.description)
		for i := 0; i < testCase.repeat; i++ {
			item := aContainer.New()
			if testCase.modifier != nil {
				testCase.modifier(item, i)
			}
			aContainer.Add(item)
			assert.NotNil(t, item, testCase.description)
		}

		if !assertly.AssertValues(t, testCase.expect, testCase.target, testCase.description) {
			data, _ := json.Marshal(testCase.target)
			fmt.Printf("%s\n", data)
		}
	}

}