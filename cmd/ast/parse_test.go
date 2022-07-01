package ast_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/assertly"
	"github.com/viant/datly/cmd/ast"
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
	}

	loader := afs.New()
	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testcase := range testcases[5:6] {
		fullURL := path.Join(testLocation, "testdata", testcase.path)
		inputFile := path.Join(fullURL, "input.txt")
		inputData, err := loader.DownloadWithURL(context.TODO(), inputFile)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		viewMeta, err := ast.Parse(string(inputData))
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		outputFile := path.Join(fullURL, "output.yaml")
		outputData, err := loader.DownloadWithURL(context.TODO(), outputFile)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		expected := &ast.ViewMeta{}
		_ = yaml.Unmarshal(outputData, expected)
		if !assertly.AssertValues(t, expected, viewMeta, testcase.description) {
			actualBytes, _ := yaml.Marshal(viewMeta)
			fmt.Println(string(actualBytes))
			fmt.Println(string(outputData))
		}
	}
}
