package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view"
	"testing"
)

func TestNewDeclarations(t *testing.T) {
	var testCases = []struct {
		description   string
		DSQL          string
		expectedSQL   string
		expectedState inference.State
		hasError      bool
	}{
		{
			description: "Query string param",
			DSQL: `
#set($_ = $TeamIDs<string>(query/tids).WithCodec(AsInts))
SELECT 1 FROM t WHERE ID IN($TeamIDs)
`,
			expectedSQL: `SELECT 1 FROM t WHERE ID IN($TeamIDs)`,
			expectedState: inference.State{
				&inference.Parameter{
					Explicit: true,
					Parameter: view.Parameter{
						Name:     "TeamIDs",
						DataType: "string",
						In: &view.Location{
							Kind: view.KindQuery,
							Name: "tids",
						},
						Codec: &view.Codec{Name: "AsInts"},
						Schema: &view.Schema{
							Cardinality: view.One,
						},
					},

					ModificationSetting: inference.ModificationSetting{},
					SQL:                 "",
					Qualifiers:          nil,
					Hint:                "",
				},
			},
		},
	}

	for _, testCase := range testCases {
		declarations, err := NewDeclarations(testCase.DSQL)
		if testCase.hasError {
			assert.NotNil(t, err, testCase.description)
			continue
		}
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.Equal(t, testCase.expectedSQL, declarations.SQL, testCase.description)
		assert.Equal(t, testCase.expectedState, declarations.State, testCase.description)

	}

}
