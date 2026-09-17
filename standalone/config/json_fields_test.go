package config

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type jsonKeyEmbedded struct{ Value int }
type jsonKeyTagged struct {
	Other int `json:"Value"`
}
type jsonKeyShadow struct {
	jsonKeyEmbedded
	Value int
}
type jsonKeyDominant struct {
	jsonKeyEmbedded
	jsonKeyTagged
}
type jsonKeyAmbiguous struct {
	A int `json:"value"`
	B int `json:"value"`
}
type jsonKeyCases struct {
	First  int `json:"VALUE"`
	Second int `json:"Value"`
}
type jsonKeyRecursive struct {
	*jsonKeyRecursive
	Value int
}

func TestJSONFieldMatchingAgreesWithDecoder(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[jsonKeyShadow](), reflect.TypeFor[jsonKeyDominant](),
		reflect.TypeFor[jsonKeyAmbiguous](), reflect.TypeFor[jsonKeyCases](),
		reflect.TypeFor[jsonKeyRecursive](),
	} {
		for _, key := range []string{"Value", "VALUE", "value", "vAlUe"} {
			t.Run(typ.Name()+"/"+key, func(t *testing.T) {
				field := jsonStructFields(typ).lookup(key)
				actual := reflect.New(typ)
				require.NoError(t, json.Unmarshal([]byte(`{"`+key+`":17}`), actual.Interface()))
				expected := reflect.New(typ)
				if field != nil {
					expected.Elem().FieldByIndex(field.index).SetInt(17)
				}
				require.Equal(t, expected.Interface(), actual.Interface())
			})
		}
	}
}

func TestJSONKeysRetainDistinctExactFieldsAndOpaqueObjects(t *testing.T) {
	typ := reflect.TypeFor[jsonKeyCases]()
	require.NoError(t, validateJSONKeys([]byte(`{"VALUE":1,"Value":2}`), typ))
	require.ErrorContains(t, validateJSONKeys([]byte(`{"VALUE":1,"value":2}`), typ), "duplicate JSON key")
	// RawMessage implements its own decoder: do not interpret its keys as fields.
	opaque := reflect.TypeFor[struct{ Body json.RawMessage }]()
	require.NoError(t, validateJSONKeys([]byte(`{"Body":{"Name":1,"name":2}}`), opaque))
	require.ErrorContains(t, validateJSONKeys([]byte(`{"Body":{"Name":1,"Name":2}}`), opaque), "duplicate JSON key")
}
