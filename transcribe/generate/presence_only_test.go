package generate

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestPresenceOnlyParameterJSONAndGeneratedTag(t *testing.T) {
	for _, value := range []bool{false, true} {
		original := &spec.Parameter{Name: "Supplied", Source: spec.BindSource{Kind: "body", Name: "channelV2"}, PresenceOnly: &value, TypeExpr: "bool"}
		encoded, err := json.Marshal(original)
		require.NoError(t, err)
		var restored spec.Parameter
		require.NoError(t, json.Unmarshal(encoded, &restored))
		field := reflect.StructField{Name: "Supplied", Type: reflect.TypeFor[bool](), Tag: reflect.StructTag(fieldTag(&restored, "Supplied", nil))}
		binding, found, err := bindly.BindingSpecFromField(field)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, binding.PresenceOnly)
		require.NoError(t, binding.ValidatePresenceOnly(field.Type))
	}
}
