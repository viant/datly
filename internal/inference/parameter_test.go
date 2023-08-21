package inference

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view/state"
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
			description: "jwt match",
			text: `SELECT USER_ID AS UserID,
		ARRAY_EXISTS(ROLE, 'READ_ONLY') AS IsReadOnly,
		ARRAY_EXISTS(FEATURE1, 'FEATURE1') AS Feature1
		FROM USER_ACL WHERE USER_ID = $criteria.AppendBinding($Unsafe.Jwt.UserID)`,
			parameter: &Parameter{
				Parameter: state.Parameter{Name: "Jwt"},
			},
			expect: true,
		},
		{
			description: "curly match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", ${CurFoos}.Values)`,
			parameter: &Parameter{
				Parameter: state.Parameter{Name: "CurFoos"},
			},
			expect: true,
		},

		{
			description: "negative match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", $CurFoosId.Values)`,
			parameter: &Parameter{
				Parameter: state.Parameter{Name: "CurFoos"},
			},
		},
		{
			description: "second match",
			text:        ` SELECT * FROM FOOS WHERE $criteria.In("ID", $CurFoosId.Values) AND $CurFoos=1 `,
			expect:      true,
			parameter: &Parameter{
				Parameter: state.Parameter{Name: "CurFoos"},
			},
		},
	}

	for _, testCase := range testCases {
		actual := testCase.parameter.IsUsedBy(testCase.text)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
