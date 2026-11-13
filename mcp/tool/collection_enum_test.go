package tool

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNestedCollectionEnumUsesNativeBodyCodec(t *testing.T) {
	type body struct {
		Channels []string `json:"channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY"`
	}
	document, err := schemaForType(reflect.TypeOf(body{}), nil)
	require.NoError(t, err)
	require.Equal(t, []any{"DISPLAY"}, document["properties"].(map[string]any)["channels"].(map[string]any)["items"].(map[string]any)["enum"])
}

func TestNestedCollectionEnumRequiresNativeLiteralAdmission(t *testing.T) {
	type body struct {
		Channels []string `json:"channels" mcpEnum:"true" choice:"DISPLAY"`
	}
	_, err := schemaForType(reflect.TypeFor[body](), nil)
	require.ErrorContains(t, err, `requires enumAccept:"literal"`)
}
