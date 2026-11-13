package registry

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestObjectConstraintWireSchemaAdmission(t *testing.T) {
	closed := false
	require.NoError(t, validateWireSchema(&spec.WireSchema{MinItems: 1, MaxItems: 50, Items: &spec.WireSchema{AdditionalProperties: &closed, AnyOf: []*spec.WireSchema{{RequiredFields: []string{"id"}}, {RequiredFields: []string{"name", "owner"}}}}}))
	for _, schema := range []*spec.WireSchema{
		{MinItems: -1}, {MinItems: 51, MaxItems: 50}, {Items: &spec.WireSchema{Type: "unsupported"}}, {AnyOf: []*spec.WireSchema{nil}}, {RequiredFields: []string{""}}, {Required: true, Items: &spec.WireSchema{Type: "integer", Format: "date"}},
	} {
		require.Error(t, validateWireSchema(schema))
	}
}
