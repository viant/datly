package openapi

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"reflect"
	"strings"
	"testing"
)

func TestWireInlineMapSchema(t *testing.T) {
	type response struct {
		Status   string         `json:"status" required:"true"`
		Extras   map[string]any `format:"inline=true"`
		Data     int            `json:"data" required:"true"`
		Optional string         `json:"optional,omitempty"`
	}
	shape, err := jsonmarshal.New(nil, nil, false, true, "", "", nil).Wire(reflect.TypeFor[response]())
	require.NoError(t, err)
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).wire(shape)
	require.NoError(t, err)
	object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
	require.NotNil(t, object.AdditionalProperties)
	require.Empty(t, object.Properties["status"].Type)
	require.Empty(t, object.Properties["optional"].Type)
	require.Equal(t, "integer", object.Properties["data"].Type)
	require.ElementsMatch(t, []string{"status", "data"}, object.Required)
	require.NotContains(t, object.Properties, "Extras")
}
