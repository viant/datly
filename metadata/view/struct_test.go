package view

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox/format"
	"reflect"
	"testing"
)

func TestFromStruct(t *testing.T) {

	type (
		Foo struct {
			ID   int
			BarID int
			Name string
		}
		Line struct {
			ID int
			BarID int
			Text string
		}
		Bar struct {
			ID    int
			Name  string
			Foo   Foo    `datly:"table=foo,on=ID=foo.BarID"`
			Lines []Line `datly:"table=bar_lines,on=bar_lines.BarID=ID"`
		}
	)

	var testCases = []struct {
		description string
		name string
		reflect.Type
		viewCaseFormat format.Case
		expect string
	}{
		{
			description: "foo type",
			name: "foo",
			Type:        reflect.TypeOf(Foo{}),
			viewCaseFormat: format.CaseUpperCamel,
			expect:      `{"Connector":"","Name":"foo","Columns":[{"@indexBy@":"Name"},{"Name":"Name","DataType":"int"},{"Name":"Name","DataType":"string"}]}`,
		},
		{
			description: "bar type with ref",
			name: "bar",
			Type:        reflect.TypeOf(Bar{}),
			viewCaseFormat: format.CaseUpperCamel,
			expect: `{"Connector":"","Name":"bar","Alias":"t","Table":"bar","Columns":[{"Name":"ID","DataType":"int"},{"Name":"Name","DataType":"string","FieldIndex":1}],"Selector":{},"Components":[{"Name":"Foo","Cardinality":"One","DataView":"Foo","On":[{"Column":"ID","RefColumn":"BarID","Param":""}]},{"Name":"Lines","Cardinality":"Many","DataView":"Lines","On":[{"Column":"ID","RefColumn":"BarID","Param":""}]}],"CaseFormat":"UpperCamel"}`,
		},
	}

	for _, testCase := range testCases {
		actual, err := FromStruct(testCase.name, testCase.Type, testCase.viewCaseFormat)
		if !assert.Nil(t, err, testCase.description) {
			fmt.Println(err)
			continue
		}
		if !assertly.AssertValues(t, testCase.expect, actual) {
			data, _ := json.Marshal(actual)
			fmt.Println(string(data))
		}
	}

}
