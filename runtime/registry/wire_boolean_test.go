package registry

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestBooleanWireSchemaValidation(t *testing.T) {
	require.NoError(t, validateWireSchema(&spec.WireSchema{Type: "boolean", Nullable: true}))
	require.ErrorContains(t, validateWireSchema(&spec.WireSchema{Type: "boolean", Format: "date"}), "does not support format")
	require.NoError(t, validateWireSchema(&spec.WireSchema{Type: "string", Format: "date"}))
	require.NoError(t, validateWireSchema(&spec.WireSchema{Type: "string", Format: "date-time"}))
	require.NoError(t, validateWireSchema(&spec.WireSchema{Type: "integer"}))
	require.ErrorContains(t, validateWireSchema(&spec.WireSchema{Type: "integer", Format: "date"}), "does not support format")
}
