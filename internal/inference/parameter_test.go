package inference

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view"
	"testing"
)

func TestParameter_IsUsedBy(t *testing.T) {

	var testCases = []struct {
		description string
		parameter   *Parameter
		text        string
		expect      bool
	}{
		{
			description: "curly match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", ${CurFoos}.Values)`,
			parameter: &Parameter{
				Parameter: view.Parameter{Name: "CurFoos"},
			},
			expect: true,
		},
		{
			description: "negative match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", $CurFoosId.Values)`,
			parameter: &Parameter{
				Parameter: view.Parameter{Name: "CurFoos"},
			},
		},
		{
			description: "second match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", $CurFoosId.Values) AND $CurFoos=1 `,
			expect:      true,
			parameter: &Parameter{
				Parameter: view.Parameter{Name: "CurFoos"},
			},
		},
	}

	for _, testCase := range testCases {
		actual := testCase.parameter.IsUsedBy(testCase.text)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
