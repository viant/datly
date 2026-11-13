package registry

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/tagly/choice"
)

func TestExplicitCollectionEnumNativeBindingAndSchema(t *testing.T) {
	field := reflect.StructField{Name: "Channels", Type: reflect.TypeFor[[]string](), Tag: `mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`}
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	require.NoError(t, err)
	schema := EnumInputSchema(enumeration, false)
	require.Equal(t, "array", schema.Type)
	require.True(t, schema.Nullable)
	require.Equal(t, []any{"DISPLAY", "VIDEO"}, schema.Items.Enum)
	for _, kind := range []string{"body", "query"} {
		binding, err := EnumBinding(bindly.BindingSpec{Location: bindstate.Location{Kind: kind}}, field)
		require.NoError(t, err)
		transform := binding.Transformer.(*enumInput)
		require.Equal(t, field.Type, transform.WireSourceType())
		var source any = []string{"DISPLAY", "VIDEO"}
		if kind == "body" {
			source = json.RawMessage(`["DISPLAY","VIDEO"]`)
			require.Equal(t, reflect.TypeFor[json.RawMessage](), binding.SourceType)
		} else {
			require.Equal(t, field.Type, binding.SourceType)
		}
		actual, err := transform.Transform(context.Background(), nil, source)
		require.NoError(t, err)
		require.Equal(t, []string{"DISPLAY", "VIDEO"}, actual)
		if kind == "body" {
			source = json.RawMessage(`["UNKNOWN"]`)
		} else {
			source = []string{"UNKNOWN"}
		}
		_, err = transform.Transform(context.Background(), nil, source)
		require.ErrorContains(t, err, "enum collection")
	}
}
