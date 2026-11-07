package output

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestJSONSchemaInlineMapUsesNativeWire(t *testing.T) {
	type envelope struct {
		Status   string         `json:"status"`
		Extras   map[string]any `format:"inline=true"`
		Data     int            `json:"data"`
		Optional string         `json:"optional,omitempty"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[envelope](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	document, err := plan.JSONSchema()
	require.NoError(t, err)
	properties := document["properties"].(map[string]any)
	require.Empty(t, properties["status"])
	require.Empty(t, properties["optional"])
	require.Equal(t, map[string]any{"type": "integer"}, properties["data"])
	require.Contains(t, document, "additionalProperties")
	for _, value := range []envelope{{}, {Status: "ok", Extras: map[string]any{"status": false, "optional": 7, "extra": nil, "data": "overwritten"}, Data: 3}} {
		encoded, err := plan.Encode(context.Background(), "json", value)
		require.NoError(t, err)
		assertJSONSchemaValid(t, document, encoded.Data)
	}
}

func TestJSONSchemaInlineMapOptionalAncestorAndGlobalOmission(t *testing.T) {
	type nested struct {
		Value int `json:"value"`
	}
	type envelope struct {
		Before string         `json:"before"`
		Extras map[string]any `format:"inline=true"`
		Nested *nested        `format:"inline=true"`
		After  int            `json:"after"`
	}
	for _, omit := range []bool{false, true} {
		component := &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{OmitEmpty: omit}}}
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[envelope](), Component: component})
		require.NoError(t, err)
		document, err := plan.JSONSchema()
		require.NoError(t, err)
		properties := document["properties"].(map[string]any)
		require.Empty(t, properties["value"], "nil inline ancestor leaves the map's value intact")
		if omit {
			require.Empty(t, properties["after"])
		}
		for _, v := range []envelope{
			{Extras: map[string]any{"value": "from map", "after": false}},
			{Extras: map[string]any{"value": false, "before": nil, "after": "from map"}, Nested: &nested{Value: 3}},
		} {
			encoded, err := plan.Encode(context.Background(), "json", v)
			require.NoError(t, err)
			assertJSONSchemaValid(t, document, encoded.Data)
		}
	}
}
