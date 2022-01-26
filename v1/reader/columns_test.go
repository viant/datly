package reader

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/v1/data"
	"testing"
)

func TestDataColumnsToNames(t *testing.T) {
	testCases := []struct {
		description string
		columns     []*data.Column
		names       []string
	}{
		{
			description: "testCase #1",
			columns: []*data.Column{
				{Name: "id"}, {Name: "name"}, {Name: "price"},
			},
			names: []string{"id", "name", "price"},
		},
	}

	for _, testCase := range testCases {
		assert.EqualValues(t, testCase.names, DataColumnsToNames(testCase.columns), testCase.description)
	}
}
