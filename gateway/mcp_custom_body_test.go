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

func TestInitializeToolArgumentsSupportsAdvertisedObjectWithCustomBodyMarshaler(t *testing.T) {
	body := state.NewParameter("Body", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(&mcpBareArrayBody{}))))
	inputType := state.Type{Schema: state.NewSchema(reflect.TypeOf(mcpCustomBodyInput{})), Parameters: state.Parameters{body}}
	inputType.SetType(reflect.TypeOf(mcpCustomBodyInput{}))
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: inputType}}}

	arguments, err := initializeToolArguments(context.Background(), component, map[string]interface{}{
		"Body": map[string]interface{}{"Ids": []interface{}{float64(17), float64(23)}},
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
