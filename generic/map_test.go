package generic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMap_Object(t *testing.T) {

	var useCases = []struct {
		description string
		index       Index
		values      []map[string]interface{}
	}{
		{
			description: "single field index",
			values: []map[string]interface{}{
				{
					"k1": "v1",
					"k2": "v3",
				},
				{
					"k1": "v10",
					"k2": "v33",
				},
			},
			index: NewIndex([]string{"k1"}),
		},
	}

	for _, useCase := range useCases {
		provider := NewProvider()
		aMap := provider.NewMap(useCase.index)
		for _, item := range useCase.values {
			aMap.Add(item)
		}

		for _, item := range useCase.values {
			key := useCase.index(item)
			object := aMap.Object(key)
			objectKey := useCase.index(object)
			assert.EqualValues(t, key, objectKey, useCase.description)
			assert.EqualValues(t, item, object.AsMap(), useCase.description)
		}

	}

}
