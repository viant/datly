package router_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/router"
	"reflect"
	"testing"
)

func TestGenerateGoStruct(t *testing.T) {
	type Bar struct {
		ID   int
		Name string
	}

	testcases := []struct {
		description string
		rType       reflect.Type
		name        string
		expected    string
	}{
		{
			description: "primitive types",
			rType:       reflect.TypeOf(0),
			name:        "Foo",
			expected:    "package generated \n\ntype Foo int",
		},
		{
			description: "primitive ptr",
			rType:       reflect.PtrTo(reflect.TypeOf(0)),
			name:        "Foo",
			expected:    "package generated \n\ntype Foo *int",
		},
		{
			description: "generated struct",
			rType: reflect.StructOf([]reflect.StructField{
				{
					Name: "Id",
					Type: reflect.TypeOf(0),
				},
				{
					Name: "Name",
					Type: reflect.TypeOf(""),
				},
				{
					Name: "Active",
					Type: reflect.TypeOf(false),
				},
			}),
			name:     "Foo",
			expected: "package generated \n\ntype Foo {\n    Id      int\n    Name    string\n    Active  bool\n}",
		},
		{
			description: "nested structs",
			rType: reflect.StructOf([]reflect.StructField{
				{
					Name: "Id",
					Type: reflect.TypeOf(0),
				},
				{
					Name: "Name",
					Type: reflect.TypeOf(""),
				},
				{
					Name: "Bar",
					Type: reflect.StructOf([]reflect.StructField{
						{
							Name: "BarId",
							Type: reflect.TypeOf(0),
						},
						{
							Name: "Price",
							Type: reflect.TypeOf(0.0),
						},
					}),
				},
			}),
			name:     "Foo",
			expected: "package generated \n\ntype Foo {\n    Id    int\n    Name  string\n    Bar   Bar\n}\n\ntype Bar {\n    BarId  int\n    Price  float64\n}",
		},
		{
			description: "tags",
			rType: reflect.StructOf([]reflect.StructField{
				{
					Name: "Id",
					Type: reflect.TypeOf(0),
					Tag:  "json:\",omitempty\"",
				},
				{
					Name: "Name",
					Type: reflect.TypeOf(""),
					Tag:  "json:\",omitempty\"",
				},
				{
					Name: "Bar",
					Type: reflect.StructOf([]reflect.StructField{
						{
							Name: "BarId",
							Type: reflect.TypeOf(0),
						},
						{
							Name: "Price",
							Type: reflect.TypeOf(0.0),
						},
					}),
				},
			}),
			name:     "Foo",
			expected: "package generated \n\ntype Foo {\n    Id    int `json:\",omitempty\"`\n    Name  string `json:\",omitempty\"`\n    Bar   Bar\n}\n\ntype Bar {\n    BarId  int\n    Price  float64\n}",
		},
		{
			description: "golang types",
			rType: reflect.StructOf([]reflect.StructField{
				{
					Name: "Id",
					Type: reflect.TypeOf(0),
					Tag:  "json:\",omitempty\"",
				},
				{
					Name: "Name",
					Type: reflect.TypeOf(""),
					Tag:  "json:\",omitempty\"",
				},
				{
					Name: "Bar",
					Type: reflect.TypeOf(Bar{}),
				},
			}),
			name:     "Foo",
			expected: "package generated \n\nimport (\n  github.com/viant/datly/router_test\n)\n\ntype Foo {\n    Id    int `json:\",omitempty\"`\n    Name  string `json:\",omitempty\"`\n    Bar   Bar\n}",
		},
	}

	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testCase := range testcases {
		goStruct := router.GenerateGoStruct(testCase.name, testCase.rType)
		assert.Equal(t, testCase.expected, goStruct, testCase.description)
	}
}
