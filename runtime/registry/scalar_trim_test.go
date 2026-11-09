package registry

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

func TestScalarTrimWireSchemaAdmissionAndPersistence(t *testing.T) {
	enumeration, err := choice.ParseEnum(`choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumInteger:"passthrough" enumCase:"upper" enumAlias:"1:1" enumAlias:"2:OLV" enumTrim:"space"`, reflect.TypeFor[int]())
	require.NoError(t, err)
	schema := EnumInputSchema(enumeration, false)
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	var restored spec.WireSchema
	require.NoError(t, json.Unmarshal(raw, &restored))
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`" DISPLAY "`, true}, {`"\t1\r\n"`, true}, {`"\u0085\u00a0\u1680\u2000\u200a\u2028\u2029\u202f\u205f\u3000olv\n"`, true},
		{`"\u180eDISPLAY"`, false}, {`"DISPLAY\ufeff"`, false}, {`" 99 "`, false}, {`99`, true}, {`" "`, false}, {`true`, false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			result, e := gojsonschema.Validate(gojsonschema.NewGoLoader(restored.Clone()), gojsonschema.NewStringLoader(tc.raw))
			require.NoError(t, e)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
		})
	}
	for _, branch := range restored.AnyOf {
		if branch.Pattern != "" {
			require.NotNil(t, branch.MinLength)
			require.Nil(t, branch.MaxLength)
		}
	}
}
