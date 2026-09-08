package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view/state"
)

type mcpBareArrayBody struct {
	Ids []int
}

func (b *mcpBareArrayBody) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '{' {
		return fmt.Errorf("legacy body accepts only a bare array")
	}
	return json.Unmarshal(data, &b.Ids)
}

func (b mcpBareArrayBody) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.Ids)
}

type mcpCustomBodyInput struct {
	Body *mcpBareArrayBody
}

type mcpNullableChild struct {
	Value   *string `json:"value,omitempty"`
	Omitted *string `json:"omitted,omitempty"`
}

type mcpNullableBody struct {
	Nested  *mcpNullableChild   `json:"nested,omitempty"`
	Items   []*mcpNullableChild `json:"items,omitempty"`
	Derived string              `json:"derived,omitempty"`
}

type mcpNullableBodyInput struct {
	Body *mcpNullableBody
}

func (i *mcpNullableBodyInput) Init(context.Context) error {
	if i.Body != nil {
		i.Body.Derived = "initialized"
	}
	return nil
}

func TestInitializeToolArgumentsPreservesExplicitNullsInTypedBody(t *testing.T) {
	body := state.NewParameter("Body", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(&mcpNullableBody{}))))
	inputType := state.Type{Schema: state.NewSchema(reflect.TypeOf(mcpNullableBodyInput{})), Parameters: state.Parameters{body}}
	inputType.SetType(reflect.TypeOf(mcpNullableBodyInput{}))
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: inputType}}}

	arguments, err := initializeToolArguments(context.Background(), component, map[string]interface{}{
		"Body": map[string]interface{}{
			"nested": map[string]interface{}{"value": nil},
			"items": []interface{}{
				map[string]interface{}{"value": nil},
				map[string]interface{}{"value": "kept"},
				nil,
			},
		},
	})
	require.NoError(t, err)
	actual, ok := arguments["Body"].(map[string]interface{})
	require.True(t, ok, "explicit-null body must retain an object wire shape, got %T", arguments["Body"])
	assert.Equal(t, "initialized", actual["derived"], "initializer-derived non-null field was lost")

	nested, ok := actual["nested"].(map[string]interface{})
	require.True(t, ok)
	value, present := nested["value"]
	assert.True(t, present, "explicit nested null was omitted")
	assert.Nil(t, value)
	_, omitted := nested["omitted"]
	assert.False(t, omitted, "omitted nullable field was fabricated")

	items, ok := actual["items"].([]interface{})
	require.True(t, ok)
	require.Len(t, items, 3)
	first, ok := items[0].(map[string]interface{})
	require.True(t, ok)
	value, present = first["value"]
	assert.True(t, present, "explicit null in array element was omitted")
	assert.Nil(t, value)
	second, ok := items[1].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "kept", second["value"])
	assert.Nil(t, items[2], "explicit null array element was not preserved")
}

func TestPreserveMCPExplicitNullsSupportsRootBodyArrays(t *testing.T) {
	initialized := []*mcpNullableChild{{}}
	original := []interface{}{map[string]interface{}{"value": nil}}
	actual, ok := preserveMCPExplicitNulls(original, initialized).([]interface{})
	require.True(t, ok)
	require.Len(t, actual, 1)
	item, ok := actual[0].(map[string]interface{})
	require.True(t, ok)
	value, present := item["value"]
	assert.True(t, present)
	assert.Nil(t, value)
}

func TestInitializeToolArgumentsSupportsAdvertisedObjectWithCustomBodyMarshaler(t *testing.T) {
	body := state.NewParameter("Body", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(&mcpBareArrayBody{}))))
	inputType := state.Type{Schema: state.NewSchema(reflect.TypeOf(mcpCustomBodyInput{})), Parameters: state.Parameters{body}}
	inputType.SetType(reflect.TypeOf(mcpCustomBodyInput{}))
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: inputType}}}

	arguments, err := initializeToolArguments(context.Background(), component, map[string]interface{}{
		"Body": map[string]interface{}{"Ids": []interface{}{float64(17), float64(23)}, "Ignored": nil},
	})
	require.NoError(t, err)
	actual, ok := arguments["Body"].(*mcpBareArrayBody)
	require.True(t, ok, "unexpected initialized body type %T", arguments["Body"])
	assert.Equal(t, []int{17, 23}, actual.Ids)
	encoded, err := json.Marshal(actual)
	require.NoError(t, err)
	assert.JSONEq(t, `[17,23]`, string(encoded), "the HTTP body must retain the component's legacy custom wire shape")
}

func TestInitializeToolArgumentsStillAcceptsBareArrayForCustomBody(t *testing.T) {
	body := state.NewParameter("Body", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(&mcpBareArrayBody{}))))
	inputType := state.Type{Schema: state.NewSchema(reflect.TypeOf(mcpCustomBodyInput{})), Parameters: state.Parameters{body}}
	inputType.SetType(reflect.TypeOf(mcpCustomBodyInput{}))
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: inputType}}}

	arguments, err := initializeToolArguments(context.Background(), component, map[string]interface{}{
		"Body": []interface{}{float64(31), float64(47)},
	})
	require.NoError(t, err)
	actual, ok := arguments["Body"].(*mcpBareArrayBody)
	require.True(t, ok, "unexpected initialized body type %T", arguments["Body"])
	assert.Equal(t, []int{31, 47}, actual.Ids)
}
