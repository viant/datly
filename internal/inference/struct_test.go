package inference

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view/state"
	"reflect"
	"testing"
)

func TestState_Compact(t *testing.T) {
	var testCases = []struct {
		deccription string
		state       State
		expectedLen int
	}{
		{
			deccription: "basic compaction",
			expectedLen: 1,
			state: State{
				{Parameter: state.Parameter{Name: "View.Limit", Schema: state.NewSchema(reflect.TypeOf(0))}},
				{Parameter: state.Parameter{Name: "View.vendor.SQL", Schema: state.NewSchema(reflect.TypeOf(""))}},
			},
		},
	}

	for _, testCase := range testCases {
		compacted, err := testCase.state.Compact("", nil)
		assert.Nil(t, err, testCase.deccription)
		assert.Equal(t, testCase.expectedLen, len(compacted))
	}
}
