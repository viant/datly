package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/inference"
	"testing"
)

func handleExprContext(state *inference.State, name string, expr *ExpressionContext) {
	parameter := &inference.Parameter{}
	parameter.Name = name
	state.Append(parameter)
}
func TestTemplate_DetectTypes(t *testing.T) {
	var testCases = []struct {
		description string
		SQL         string
		state       inference.State
		handler     func(state *inference.State, parameter string, expr *ExpressionContext)
		expect      []string
	}{{
		SQL:     `INSERT INTO t (ID, NAME) VALUES($Id, $Name)`,
		handler: handleExprContext,
		expect:  []string{"Id", "Name"},
	},
		{
			SQL: `
#set($X = 1)
SELECT * FROM T WHERE ID = $Id AND Status = $X
`,
			handler: handleExprContext,
			expect:  []string{"Id"},
		},
	}

	for _, testCase := range testCases {
		template, err := NewTemplate(testCase.SQL, &testCase.state)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		template.DetectTypes(testCase.handler)
		var actual []string
		for _, param := range testCase.state {
			actual = append(actual, param.Name)
		}
		assert.EqualValues(t, testCase.expect, actual)
	}

}
