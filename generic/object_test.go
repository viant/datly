package generic

import (
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"log"
	"strings"
	"testing"
)

func TestObject_SetValue(t *testing.T) {

	var useCases = []struct {
		description string
		values      map[string]interface{}
	}{
		{
			description: "single value",
			values: map[string]interface{}{
				"K1": "123",
			},
		},
		{
			description: "multi value",
			values: map[string]interface{}{
				"K1": "123",
				"K2": nil,
				"K3": 4.5,
			},
		},
	}

	for _, useCase := range useCases {

		provider := NewProvider()
		object := provider.NewObject()

		for k, v := range useCase.values {
			object.SetValue(k, v)
		}
		for k, v := range useCase.values {
			assert.Equal(t, v, object.Value(k), useCase.description)
		}

	}

}

func TestObject_Set(t *testing.T) {

	var useCases = []struct {
		description string
		values      map[string]interface{}
	}{
		{
			description: "single value",
			values: map[string]interface{}{
				"K1": "123",
			},
		},
		{
			description: "multi value",
			values: map[string]interface{}{
				"K1": "123",
				"K2": nil,
				"K3": 4.5,
			},
		},
	}

	for _, useCase := range useCases {
		provider := NewProvider()
		object := provider.NewObject()
		object.Init(useCase.values)
		for k, v := range useCase.values {
			assert.Equal(t, v, object.Value(k), useCase.description)
		}

	}

}

func TestObject_MarshalJSONObject(t *testing.T) {

	var useCases = []struct {
		description string
		values      map[string]interface{}
	}{
		{
			description: "single value",
			values: map[string]interface{}{
				"K1":    "123",
				"array": []int{1, 2, 3},
			},
		},
		{
			description: "multi value",
			values: map[string]interface{}{
				"K1": "123",
				"K2": nil,
				"K3": 4.5,
			},
		},
	}

	for _, useCase := range useCases {
		provider := NewProvider()
		object := provider.NewObject()
		object.Init(useCase.values)

		b := strings.Builder{}
		enc := gojay.NewEncoder(&b)
		if err := enc.Encode(object); err != nil {
			log.Fatal(err)
		}
		assertly.AssertValues(t, object.AsMap(), b.String(), useCase.description)
	}

}
