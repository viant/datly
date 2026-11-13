package tool

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/body"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"strings"
	"testing"
)

type jsonxConversionSchemaRow struct {
	Enabled *bool    `json:"enabled" jsonx:"intToBool" format:"inputNames=active"`
	Catalog *string  `json:"catalog" jsonx:"numberToString"`
	Names   []string `json:"names" jsonx:"commaStringToSlice"`
	Billed  *string  `json:"billed" jsonx:"boolIntToYN"`
	Plain   *bool    `json:"plain"`
}

func TestJSONXConversionSchemaDomains(t *testing.T) {
	projected, err := (&schemaProjector{nativeBody: true}).argument(reflect.TypeFor[jsonxConversionSchemaRow](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`{"active":1}`, true}, {`{"active":2}`, false}, {`{"enabled":0}`, true}, {`{"enabled":1}`, true}, {`{"enabled":true}`, true}, {`{"enabled":2}`, false}, {`{"enabled":"1"}`, false},
		{`{"catalog":123.25}`, true}, {`{"catalog":"custom"}`, true}, {`{"catalog":false}`, false},
		{`{"names":"a, b"}`, true}, {`{"names":["a","b"]}`, true}, {`{"names":[1]}`, false}, {`{"names":12}`, false},
		{`{"billed":false}`, true}, {`{"billed":-7}`, true}, {`{"billed":"y"}`, true}, {`{"billed":"true"}`, false},
		{`{"enabled":null,"catalog":null,"names":null,"billed":null}`, true}, {`{"plain":1}`, false},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			result, err := validator.Validate(gojsonschema.NewStringLoader(tc.payload))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
		})
	}
}

func TestJSONXConversionOwnerRefinements(t *testing.T) {
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{
		"Body.enabled": {Type: "boolean", Required: true},
		"Body.names":   {Type: "array", MinItems: 1, MaxItems: 2},
	}}).argument(reflect.TypeFor[jsonxConversionSchemaRow](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`{}`, false}, {`{"enabled":null}`, false}, {`{"enabled":1}`, true}, {`{"active":1}`, true}, {`{"active":null}`, false},
		{`{"enabled":true,"names":[]}`, false}, {`{"enabled":true,"names":["a"]}`, true}, {`{"enabled":true,"names":"a,b"}`, true},
		{`{"enabled":true,"names":["a","b","c"]}`, false},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			result, err := validator.Validate(gojsonschema.NewStringLoader(tc.payload))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
		})
	}
}

func TestJSONXConversionNativeToolBinding(t *testing.T) {
	type input struct {
		Body *jsonxConversionSchemaRow `parameter:"Body,kind=body"`
	}
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "JSONXConversions"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "jsonxConversions"}, Contract: contract})
	require.NoError(t, err)
	scope, err := plan.Scope(map[string]any{"body": map[string]any{"active": 1, "catalog": 123.25, "names": "a, b, ,c", "billed": -7}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*jsonxConversionSchemaRow](), "")
	require.NoError(t, err)
	require.True(t, found)
	actual := value.(*jsonxConversionSchemaRow)
	require.Equal(t, true, *actual.Enabled)
	require.Equal(t, "123.25", *actual.Catalog)
	require.Equal(t, []string{"a", "b", "c"}, actual.Names)
	require.Equal(t, "Y", *actual.Billed)
}

func TestJSONXConversionNestedSchemas(t *testing.T) {
	type container struct {
		Rows []jsonxConversionSchemaRow           `json:"rows"`
		ByID map[string]*jsonxConversionSchemaRow `json:"byId"`
	}
	projected, err := (&schemaProjector{nativeBody: true}).argument(reflect.TypeFor[container](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"rows":[{"active":1,"names":"a,b"}],"byId":{"x":{"active":0,"catalog":42}}}`, true},
		{`{"rows":[{"active":2}]}`, false}, {`{"byId":{"x":{"active":2}}}`, false}, {`{"byId":{"x":{"names":[42]}}}`, false},
	} {
		result, err := validator.Validate(gojsonschema.NewStringLoader(tc.raw))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%s %v", tc.raw, result.Errors())
	}
}

func TestJSONXNullOmissionAndArrayCells(t *testing.T) {
	type row struct {
		Value float64  `json:"value" jsonx:"null=omit"`
		Names []string `json:"names" jsonx:"commaStringToSlice"`
	}
	projected, err := (&schemaProjector{nativeBody: true}).argument(reflect.TypeFor[row](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, raw := range []string{`{"value":null}`, `{"names":[null,"a"]}`} {
		source, err := body.New([]byte(raw), "application/json", nil)
		require.NoError(t, err)
		value, found, err := source.Value(t.Context(), reflect.TypeFor[*row](), "")
		require.NoError(t, err)
		require.True(t, found)
		if strings.Contains(raw, "names") {
			require.Equal(t, []string{"", "a"}, value.(*row).Names)
		}
		result, err := validator.Validate(gojsonschema.NewStringLoader(raw))
		require.NoError(t, err)
		if !result.Valid() {
			t.Errorf("%s %v", raw, result.Errors())
		}
	}
}
