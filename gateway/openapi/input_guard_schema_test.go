package openapi

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"reflect"
	"strings"
	"testing"
)

func TestNativeInputGuardSchema(t *testing.T) {
	type child struct {
		Value *int `json:"value" format:"inputUnless=../Canonical" jsonx:"boolToInt" required:"true"`
	}
	type parent struct {
		Canonical *int   `json:"canonical"`
		Child     *child `json:"child"`
	}
	for _, input := range []bool{true, false} {
		schemas := openapi3.Schemas{}
		_, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[parent](), input)
		require.NoError(t, err)
		var object *openapi3.Schema
		for _, s := range schemas {
			if s.Properties["value"] != nil {
				object = s
				break
			}
		}
		require.NotNil(t, object)
		value := object.Properties["value"]
		if input {
			require.NotContains(t, object.Required, "value")
			require.Empty(t, value.Type)
			require.Empty(t, value.AnyOf)
			require.True(t, strings.Contains(value.Description, "runtime"))
		} else {
			require.Equal(t, "integer", value.Type)
		}
	}
}
