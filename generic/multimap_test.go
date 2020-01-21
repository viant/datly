package generic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMultimap_Slice(t *testing.T) {

	var useCases = []struct {
		description string
		index       Index
		values      []map[string]interface{}
		expectSizes map[string]int
	}{
		{
			description: "single field  imdex",
			values: []map[string]interface{}{
				{
					"k1": "v1",
					"k2": "v01",
				},
				{
					"k1": "v1",
					"k2": "v02",
				},
				{
					"k1": "v1",
					"k2": "v02",
				},
				{
					"k1": "v2",
					"k2": "v22",
				},
			},
			expectSizes: map[string]int{
				"v1": 3,
				"v2": 1,
			},
			index: NewIndex([]string{"k1"}),
		},
	}

	for _, useCase := range useCases {
		provider := NewProvider()
		aMap := provider.NewMultimap(useCase.index)
		for _, item := range useCase.values {
			aMap.Add(item)
		}

		for _, item := range useCase.values {
			key := useCase.index(item)
			slice := aMap.Slice(key)
			expectSize := useCase.expectSizes[key]
			assert.EqualValues(t, expectSize, slice.Size(), useCase.description)

		}

	}

}
