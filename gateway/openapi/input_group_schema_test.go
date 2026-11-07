package openapi

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"reflect"
	"strings"
	"testing"
)

func TestNativeInputGroupSchemaPrecedence(t *testing.T) {
	type row struct {
		Low  *int `json:"low" format:"inputGroup=source,inputPriority=0,inputCase=exact" jsonx:"boolToInt" required:"true"`
		High *int `json:"high" format:"inputGroup=source,inputPriority=1,inputCase=exact"`
	}
	for _, input := range []bool{true, false} {
		schemas := openapi3.Schemas{}
		ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[row](), input)
		require.NoError(t, err)
		object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
		if input {
			require.NotContains(t, object.Required, "low")
			for _, name := range []string{"low", "high"} {
				property := object.Properties[name]
				require.Empty(t, property.Type)
				require.Empty(t, property.AnyOf)
				require.Contains(t, property.Description, "runtime")
			}
		} else {
			require.Equal(t, "integer", object.Properties["low"].Type)
		}
	}
}
