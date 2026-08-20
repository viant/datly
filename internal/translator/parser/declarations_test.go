package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/state"
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
					Parameter: state.Parameter{
						Name: "TeamIDs",
						In: &state.Location{
							Kind: state.KindQuery,
							Name: "tids",
						},
						Output: &state.Codec{Name: "AsInts", Args: []string{}},
						Schema: &state.Schema{
							Cardinality: state.One,
							DataType:    "string",
						},
						Required: &[]bool{false}[0],
					},

					ModificationSetting: inference.ModificationSetting{},
					SQL:                 "",
					Hint:                "",
				},
			},
		},
		{
			description: "Query string param with #define alias",
			DSQL: `
#define($_ = $TeamIDs<string>(query/tids).WithCodec(AsInts))
SELECT 1 FROM t WHERE ID IN($TeamIDs)
`,
			expectedSQL: `SELECT 1 FROM t WHERE ID IN($TeamIDs)`,
			expectedState: inference.State{
				&inference.Parameter{
					Explicit: true,
					Parameter: state.Parameter{
						Name: "TeamIDs",
						In: &state.Location{
							Kind: state.KindQuery,
							Name: "tids",
						},
						Output: &state.Codec{Name: "AsInts", Args: []string{}},
						Schema: &state.Schema{
							Cardinality: state.One,
							DataType:    "string",
						},
						Required: &[]bool{false}[0],
					},
					ModificationSetting: inference.ModificationSetting{},
					SQL:                 "",
					Hint:                "",
				},
			},
		},
		{
			description: "Query string param with #settings alias",
			DSQL: `
#settings($_ = $TeamIDs<string>(query/tids).WithCodec(AsInts))
SELECT 1 FROM t WHERE ID IN($TeamIDs)
`,
			expectedSQL: `SELECT 1 FROM t WHERE ID IN($TeamIDs)`,
			expectedState: inference.State{
				&inference.Parameter{
					Explicit: true,
					Parameter: state.Parameter{
						Name: "TeamIDs",
						In: &state.Location{
							Kind: state.KindQuery,
							Name: "tids",
						},
						Output: &state.Codec{Name: "AsInts", Args: []string{}},
						Schema: &state.Schema{
							Cardinality: state.One,
							DataType:    "string",
						},
						Required: &[]bool{false}[0],
					},

					ModificationSetting: inference.ModificationSetting{},
					SQL:                 "",
					Hint:                "",
				},
			},
		},
	}

	for _, testCase := range testCases {
		declarations, err := NewDeclarations(testCase.DSQL, nil)
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
