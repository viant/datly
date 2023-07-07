package inference

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlparser"
	"testing"
)

func TestTemplate_DetectParameters(t *testing.T) {
	var testCases = []struct {
		description  string
		template     *Template
		expectParams map[string]string
	}{
		{
			description: "basic selector",
			template: &Template{
				Table: &Table{
					Name:      "bar",
					Namespace: "b",
					Columns: sqlparser.Columns{
						{
							Name: "id",
							Type: "int",
						},
					},
				},
				SQL: `SELECT 1 FROM bar b WHERE b.ID = $Id
`,
			},
			expectParams: map[string]string{
				"Id": "int",
			},
		},
		{
			description: "if statement",
			template: &Template{
				Table: &Table{
					Name:      "bar",
					Namespace: "b",
					Columns: sqlparser.Columns{
						{
							Name: "id",
							Type: "int",
						},
					},
				},
				SQL: `SELECT 1 FROM bar b WHERE  1 = 1
                        #if($Has.Id)
                        AND b.ID = $Id
						#end
`,
			},
			expectParams: map[string]string{
				"Id": "int",
			},
		},
	}

	for _, testCase := range testCases[len(testCases)-1:] {
		err := testCase.template.DetectParameters()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actual := map[string]string{}
		for _, param := range testCase.template.State.Implicit() {
			actual[param.Name] = param.Schema.DataType
		}
		assert.EqualValues(t, testCase.expectParams, actual)
	}
}
