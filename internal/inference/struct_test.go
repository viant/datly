package inference

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view"
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
				{Parameter: view.Parameter{Name: "View.Limit", Schema: view.NewSchema(reflect.TypeOf(0))}},
				{Parameter: view.Parameter{Name: "View.vendor.SQL", Schema: view.NewSchema(reflect.TypeOf(""))}},
			},
		},
	}

	for _, testCase := range testCases {
		compacted, err := testCase.state.Compact("")
		assert.Nil(t, err, testCase.deccription)
		assert.Equal(t, testCase.expectedLen, len(compacted))
	}
}
