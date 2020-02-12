package generic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestanArray_Add(t *testing.T) {
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
		anArray := provider.NewArray()
		for _, v := range useCase.values {
			anArray.Add(v)
		}
		count := 0
		anArray.Objects(func(object *Object) (b bool, err error) {
			assert.EqualValues(t, useCase.values[count], object.AsMap())
			count++
			return true, nil
		})
		assert.Equal(t, len(useCase.values), count)

	}

}

func TestanArray_Objects(t *testing.T) {
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
		anArray := provider.NewArray()
		anArray.Add(useCase.values)
		assert.EqualValues(t, 1, anArray.Size(), useCase.description)

		anArray.Objects(func(item *Object) (b bool, err error) {
			for k, v := range useCase.changes {
				item.SetValue(k, v)
			}
			return true, nil
		})

		count := 0
		anArray.Objects(func(item *Object) (b bool, err error) {
			count++
			assert.EqualValues(t, expect, item.AsMap(), useCase.description)
			return true, nil
		})

		assert.EqualValues(t, 1, count, useCase.description)

	}

}

func TestArray_AddObject(t *testing.T) {
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
		anArray := provider.NewArray()
		for _, v := range useCase.values {
			anArray.Add(v)
		}

		compacted := anArray.Compact()

		array2 := NewProvider().NewArray()
		compacted.Update(array2)
		count := 0
		array2.Objects(func(object *Object) (b bool, err error) {
			assert.EqualValues(t, useCase.values[count], object.AsMap())
			count++
			return true, nil
		})
		assert.Equal(t, len(useCase.values), count)

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
