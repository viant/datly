package ast_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/assertly"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"path"
	"testing"
)

func TestParse(t *testing.T) {
	testLocation := toolbox.CallerDirectory(3)

	testcases := []struct {
		description string
		path        string
		uriParams   map[string]bool
		hints       option.ParameterHints
	}{
		{
			description: "basic",
			path:        "case001",
		},
		{
			description: "inner where",
			path:        "case002",
		},
		{
			description: "double where",
			path:        "case003",
		},
		{
			description: "template header",
			path:        "case004",
		},
		{
			description: "imply params",
			path:        "case005",
		},
		{
			description: "foreach and set",
			path:        "case006",
		},
		{
			description: "detect params only int statements",
			path:        "case007",
		},
		{
			description: "param type hint",
			path:        "case008",
			hints: option.ParameterHints{
				{Parameter: "quantity", Hint: `/* {"DataType": "time.Time"} */`},
			},
		},
		{
			description: "uri params",
			path:        "case009",
			uriParams: map[string]bool{
				"tID": true,
			},
		},
	}

	loader := afs.New()
	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testcase := range testcases {
		fullURL := path.Join(testLocation, "testdata", testcase.path)
		inputFile := path.Join(fullURL, "input.txt")
		inputData, err := loader.DownloadWithURL(context.TODO(), inputFile)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		viewMeta, err := ast.Parse(string(inputData), &option.Route{URIParams: testcase.uriParams}, testcase.hints)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		outputFile := path.Join(fullURL, "output.yaml")
		outputData, err := loader.DownloadWithURL(context.TODO(), outputFile)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		actualMeta, _ := yaml.Marshal(viewMeta)
		expected := normalize(outputData)
		actual := normalize(actualMeta)
		if !assertly.AssertValues(t, string(expected), string(actual), testcase.description) {
			fmt.Println(string(expected))
			fmt.Println(string(actual))
		}
	}
}

func normalize(b []byte) []byte {
	aMap := map[string]interface{}{}

	_ = yaml.Unmarshal(b, aMap)
	result, _ := yaml.Marshal(aMap)
	return result
}

func TestExtractCondBlock(t *testing.T) {
	var testCases = []struct {
		description string
		SQL         string
		expect      string
		exprs       []string
	}{
		{
			SQL: `SELECT * FROM x WHERE 1=1 #if($Has.Id) 
	id = $Id
#end`,
			expect: "SELECT * FROM x WHERE 1=1 ",
			exprs:  []string{"id = $Id"},
		},
	}

	for _, testCase := range testCases {
		actual, exprs := ast.ExtractCondBlock(testCase.SQL)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
		assert.EqualValues(t, testCase.exprs, exprs, testCase.description)
	}
}
