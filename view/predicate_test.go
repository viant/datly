package view

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_ComputePredicate(t *testing.T) {

	stringSchema := NewSchema(reflect.TypeOf(""))
	var testCases = []struct {
		description        string
		PredicateParameter *Parameter
		stateValue         string
		Registry           PredicateRegistry
		Expected           interface{}
	}{
		{

			description: "abc",
			PredicateParameter: &Parameter{Name: "P1",
				In: &Location{Kind: KindPredicate},
				Fields: Parameters{
					&Parameter{Name: "ProductName", Schema: stringSchema, In: &Location{Kind: KindQuery, Name: "pname"},
						Predicate: &ParameterPredicate{Name: "exists", Args: []string{
							"t.name",
							"Product",
							"t.id",
							"p.product_id",
						}},
					},
					&Parameter{Name: "Country", Schema: stringSchema, In: &Location{Kind: KindQuery, Name: "country"},
						Predicate: &ParameterPredicate{Name: "exists", Args: []string{
							"t.name",
							"Country",
							"t.id",
							"p.country_id",
						}},
					},
					&Parameter{Name: "Foo", Schema: stringSchema, In: &Location{Kind: KindQuery, Name: "bar"},
						Predicate: &ParameterPredicate{Name: "exists", Args: []string{
							"t.name",
							"Bar",
							"t.id",
							"p.bar_id",
						}},
					},
				},
				Schema: &Schema{
					Name: "P1",
				},
			},
			stateValue: `
{
	"P1":{
		"ProductName":"ipad",
		"Country":"Bar",
		"Has": {
			"ProductName":true,
			"Country":true
		}
	}
}`,
		},
	}

	for _, testCase := range testCases {
		parameter := testCase.PredicateParameter
		parameterType := parameter.PredicateType()
		paramState := reflect.StructOf([]reflect.StructField{
			{Name: "P1", Type: parameterType},
		})
		state := reflect.New(paramState).Interface()
		err := json.Unmarshal([]byte(testCase.stateValue), state)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		fmt.Printf("%T %v\n", state, state)
		//ComputePredicate
	}

}
