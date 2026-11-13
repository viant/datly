package registry

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/tagly/choice"
)

func TestEnumNullSchemaMatchesNativeBinding(t *testing.T) {
	for _, reject := range []bool{false, true} {
		tag := reflect.StructTag(`choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"literal" enumStyle:"literal"`)
		if reject {
			tag += ` enumNull:"reject"`
		}
		enum, err := choice.ParseEnum(tag, reflect.TypeFor[*int]())
		require.NoError(t, err)
		require.Equal(t, !reject, EnumInputSchema(enum, true).Nullable)
		binding, err := EnumBinding(bindly.BindingSpec{Location: bindstate.Location{Kind: "body", In: "mode"}}, reflect.StructField{Name: "Mode", Type: reflect.TypeFor[int](), Tag: tag})
		require.NoError(t, err)
		value, err := binding.Transformer.Transform(context.Background(), nil, nil)
		if reject {
			require.ErrorIs(t, err, choice.ErrNullInput)
		} else {
			require.NoError(t, err)
			require.Equal(t, 0, value)
		}
		value, err = binding.Transformer.Transform(context.Background(), nil, "ASAP")
		require.NoError(t, err)
		require.Equal(t, 0, value)
	}
}
func TestEnumNullScalarUnionPointerSchema(t *testing.T) {
	for _, reject := range []bool{false, true} {
		tag := reflect.StructTag(`choice:"1:ONE" enumAccept:"scalar" enumStyle:"scalar"`)
		if reject {
			tag += ` enumNull:"reject"`
		}
		enum, err := choice.ParseEnum(tag, reflect.TypeFor[*choice.ScalarValue]())
		require.NoError(t, err)
		for _, nullable := range []bool{false, true} {
			require.Equal(t, !reject, EnumInputSchema(enum, nullable).Nullable)
		}
	}
}
