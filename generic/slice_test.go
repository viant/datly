package generic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSlice_Add(t *testing.T) {
	var useCases = []struct {
		description string
		values      []map[string]interface{}
	}{
		{
			description: "single item value",
			values: []map[string]interface{}{
				{
					"K1": "123",
					"K2": "123",
				},
			},
		},
		{
			description: "single item value",
			values: []map[string]interface{}{
				{
					"K21": "1",
					"K2":  "2",
					"K7":  "3",
				},
				{
					"K21": "4",
					"K2":  "5",
					"K7":  "",
				},
			},
		},
	}

	for _, useCase := range useCases {

		provider := NewProvider()
		slice := provider.NewSlice()
		for _, v := range useCase.values {
			slice.Add(v)
		}
		count := 0
		slice.Objects(func(object *Object) (b bool, err error) {
			assert.EqualValues(t, useCase.values[count], object.AsMap())
			count++
			return true, nil
		})
		assert.Equal(t, len(useCase.values), count)

	}

}

func TestSlice_Objects(t *testing.T) {
	var useCases = []struct {
		description string
		values      map[string]interface{}
		changes     map[string]interface{}
	}{
		{
			description: "single value",
			values: map[string]interface{}{
				"k1": "v1",
				"k2": "v3",
			},
			changes: map[string]interface{}{
				"k1": "v10",
				"k3": "v3",
			},
		},
	}

	for _, useCase := range useCases {
		expect := mergeMap(useCase.values, useCase.changes)
		provider := NewProvider()
		slice := provider.NewSlice()
		slice.Add(useCase.values)
		assert.EqualValues(t, 1, slice.Size(), useCase.description)

		slice.Objects(func(item *Object) (b bool, err error) {
			for k, v := range useCase.changes {
				item.SetValue(k, v)
			}
			return true, nil
		})

		count := 0
		slice.Objects(func(item *Object) (b bool, err error) {
			count++
			assert.EqualValues(t, expect, item.AsMap(), useCase.description)
			return true, nil
		})

		assert.EqualValues(t, 1, count, useCase.description)

	}

}

func mergeMap(maps ...map[string]interface{}) map[string]interface{} {
	expect := map[string]interface{}{}
	for _, aMap := range maps {
		for k, v := range aMap {
			expect[k] = v
		}
	}
	return expect
}
