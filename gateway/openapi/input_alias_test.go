package openapi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/tagly/format"
	"github.com/xeipuuv/gojsonschema"
)

func TestInputAliasesDoNotBecomeResponseProperties(t *testing.T) {
	type constraint struct {
		Limit int `json:"limit"`
	}
	type request struct {
		Frequency *constraint `json:"frequencyCapping,omitempty" format:"inputNames=frequencyCap"`
	}
	for _, input := range []bool{true, false} {
		name := "output"
		if input {
			name = "input"
		}
		t.Run(name, func(t *testing.T) {
			schemas := openapi3.Schemas{}
			ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[request](), input)
			require.NoError(t, err)
			object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
			require.NotNil(t, object)
			require.Contains(t, object.Properties, "frequencyCapping")
			if input {
				require.Contains(t, object.Properties, "frequencyCap")
				require.Equal(t, object.Properties["frequencyCapping"], object.Properties["frequencyCap"])
			} else {
				require.NotContains(t, object.Properties, "frequencyCap")
			}
		})
	}
}

func TestRequiredInputAliasAlternatives(t *testing.T) {
	type request struct {
		Name string `json:"name" required:"true" format:"inputNames=legacyName|oldName"`
		ID   int    `json:"id" required:"true"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[request](), true)
	require.NoError(t, err)
	object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
	require.Equal(t, []string{"id"}, object.Required)
	require.Len(t, object.AllOf, 1)
	require.Len(t, object.AllOf[0].AnyOf, 3)
	for i, name := range []string{"name", "legacyName", "oldName"} {
		require.Equal(t, []string{name}, object.AllOf[0].AnyOf[i].Required)
		require.Equal(t, "string", object.Properties[name].Type)
	}
	document := map[string]any{"components": map[string]any{"schemas": schemas}, "allOf": []any{ref}}
	for _, tc := range []struct {
		body  map[string]any
		valid bool
	}{
		{map[string]any{"name": "a", "id": 1}, true},
		{map[string]any{"legacyName": "a", "id": 1}, true},
		{map[string]any{"oldName": "a", "id": 1}, true},
		{map[string]any{"id": 1}, false},
		{map[string]any{"legacyName": "a"}, false},
		{map[string]any{"legacyName": 1, "id": 1}, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(document), gojsonschema.NewGoLoader(tc.body))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "body=%v errors=%v", tc.body, result.Errors())
	}
}

func TestInputAliasCollisionFailsDiscovery(t *testing.T) {
	type request struct {
		Name string `json:"name" format:"inputNames=id"`
		ID   int    `json:"id"`
	}
	_, err := newSchemaBuilder(openapi3.Schemas{}).schema(reflect.TypeFor[request](), true)
	require.ErrorContains(t, err, "conflicts with canonical")
}

func TestInputAliasPolicyDoesNotRejectIgnoredValues(t *testing.T) {
	type constraint struct {
		Limit int `json:"limit"`
	}
	type request struct {
		Frequency *constraint `json:"frequencyCapping,omitempty" format:"inputNames=frequencyCap,inputNamesPriority=canonical,inputNamesKinds=object|null,inputNamesCase=fold"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[request](), true)
	require.NoError(t, err)
	document := map[string]any{"components": map[string]any{"schemas": schemas}, "allOf": []any{ref}}
	for _, body := range []map[string]any{
		{"frequencyCap": 5},
		{"frequencyCapping": map[string]any{"limit": 2}, "frequencyCap": map[string]any{"limit": "ignored"}},
		{"FREQUENCYCAPPING": map[string]any{"limit": 2}, "frequencyCap": map[string]any{"limit": "ignored"}},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(document), gojsonschema.NewGoLoader(body))
		require.NoError(t, err)
		require.True(t, result.Valid(), "runtime accepts %v: %v", body, result.Errors())
	}
}

func TestAliasPriorityDoesNotConstrainDiscardedCanonical(t *testing.T) {
	property := &openapi3.Schema{Type: "integer"}
	object := &openapi3.Schema{Type: "object", Properties: openapi3.Schemas{"flightId": property}}
	required := projectInputAliases(object, "flightId", property, &format.Tag{InputNames: "CampaignFlightId", InputNamesPriority: "alias", InputNamesCase: "fold"}, true)
	require.False(t, required)
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(object), gojsonschema.NewGoLoader(map[string]any{"flightId": "discarded", "CampaignFlightId": 2}))
	require.NoError(t, err)
	require.True(t, result.Valid(), "%v", result.Errors())
	require.Equal(t, "integer", property.Type)
}
