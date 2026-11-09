package registry

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/tagly/choice"
	"reflect"
	"testing"
)

func TestScalarUnionInputNullability(t *testing.T) {
	enumeration, err := choice.ParseEnum(`choice:"4237:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`, reflect.TypeFor[choice.ScalarValue]())
	require.NoError(t, err)
	schema := EnumInputSchema(enumeration, false)
	require.True(t, schema.Nullable, "the enum domain accepts null even with non-pointer storage")
	require.Empty(t, schema.Type)
	require.Empty(t, schema.Enum)
}
