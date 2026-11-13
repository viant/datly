package tool

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNestedCollectionEnumRequiresOwningMCPArgument(t *testing.T) {
	type body struct {
		Channels []string `json:"channels" mcpEnum:"true" choice:"DISPLAY"`
	}
	_, err := schemaForType(reflect.TypeOf(body{}), nil)
	require.ErrorContains(t, err, "requires a direct named or flattened body argument")
}
