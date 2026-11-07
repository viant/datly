package openapi_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type stagedRequest struct {
	CustomDealID int `json:"customDealId"`
}
type stagedBodyInput struct {
	Supplied bool           `parameter:"Supplied,kind=body,in=channelV2,presenceOnly=true" internal:"true"`
	Request  *stagedRequest `parameter:"Request,kind=body"`
	Channel  int            `parameter:"Channel,kind=body,in=channelV2,scope=channel" choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumInteger:"passthrough" enumCase:"upper" enumTrim:"space"`
}

func TestStagedNamedBodySchemaComposition(t *testing.T) {
	entry := (fixture{input: reflect.TypeFor[stagedBodyInput]()}).registration(t)
	doc, err := (openapi.Generator{}).Generate(t.Context(), documentRequest(entry))
	require.NoError(t, err)
	schema := doc.Paths["/records"].Post.RequestBody.Content["application/json"].Schema
	a := documentAssertion{t, doc}
	schema = a.deref(schema)
	if len(schema.AnyOf) == 2 {
		schema = a.deref(schema.AnyOf[0])
	}
	require.Contains(t, schema.Properties, "customDealId")
	require.NotContains(t, schema.Properties, "Supplied")
	require.Contains(t, schema.Properties, "channelV2")
	require.NotContains(t, schema.Required, "channelV2")
	require.NotEmpty(t, schema.Properties["channelV2"].AnyOf)
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"customDealId":7,"channelV2":" DISPLAY "}`, true},
		{`{"customDealId":7,"channelV2":99}`, true},
		{`{"customDealId":7}`, true},
		{`{"customDealId":7,"channelV2":"99"}`, false},
		{`{"customDealId":7,"channelV2":{}}`, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.raw))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), tc.raw)
	}
	// Composition must not add staged members to the reusable whole-body component.
	for _, component := range doc.Components.Schemas {
		if _, ok := component.Properties["customDealId"]; ok {
			require.NotContains(t, component.Properties, "channelV2")
		}
	}

}

func TestStagedBodyRejectsAmbiguousAuthority(t *testing.T) {
	type folded struct {
		Channel int `json:"CHANNELV2"`
	}
	type alias struct {
		Other int `json:"other" format:"inputNames=channelV2"`
	}
	type excluded struct {
		Other int `json:"other" format:"inputNames=channelV2,inputNamesExclude=channelV2"`
	}
	cases := []struct {
		name  string
		whole reflect.Type
		tag   string
	}{
		{"fold", reflect.TypeFor[*folded](), `parameter:"kind=body,in=channelV2,scope=channel"`},
		{"alias", reflect.TypeFor[*alias](), `parameter:"kind=body,in=channelV2,scope=channel"`},
		{"excluded alias", reflect.TypeFor[*excluded](), `parameter:"kind=body,in=channelV2,scope=channel"`},
		{"non object", reflect.TypeFor[[]int](), `parameter:"kind=body,in=channelV2,scope=channel"`},
		{"unscoped unchanged", reflect.TypeFor[*stagedRequest](), `parameter:"kind=body,in=channelV2"`},
		{"when", reflect.TypeFor[*stagedRequest](), `parameter:"kind=body,in=channelV2,scope=channel,when=Enabled"`},
		{"with", reflect.TypeFor[*stagedRequest](), `parameter:"kind=body,in=channelV2,scope=channel,with=Other"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Request", Type: tc.whole, Tag: `parameter:"kind=body"`}, {Name: "Channel", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(tc.tag)}})
			entry, err := (fixture{input: typ}).build()
			if err != nil {
				return
			}
			_, err = (openapi.Generator{}).Generate(t.Context(), documentRequest(entry))
			require.Error(t, err)
		})
	}
}

func TestPresenceProjectionRequiresPublicTypedOwner(t *testing.T) {
	type input struct {
		Supplied bool `parameter:"kind=body,in=channelV2,presenceOnly=true" internal:"true"`
	}
	entry := (fixture{input: reflect.TypeFor[input]()}).registration(t)
	_, err := (openapi.Generator{}).Generate(t.Context(), documentRequest(entry))
	require.ErrorContains(t, err, "no public static typed body owner")
}
