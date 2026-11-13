package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestAuthoredNullPolicyWithConstraintOnlySchema(t *testing.T) {
	type request struct {
		Limit   *int     `json:"limit" format:"nullable=false"`
		Queries []string `json:"queries"`
	}
	type flat struct {
		Body *request `anonymous:"true"`
	}
	type direct struct {
		Limit *int `json:"limit" format:"nullable=false"`
	}
	minimum, maximum := json.Number("1"), json.Number("50")
	for _, tc := range []struct {
		name    string
		typ     reflect.Type
		binding bindly.BindingSpec
	}{
		{"flattened", reflect.TypeFor[flat](), bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}, Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Body.Limit": {Minimum: &minimum, Maximum: &maximum}, "Body.Queries": {MaxItems: 10}}}}},
		{"direct", reflect.TypeFor[direct](), bindly.BindingSpec{Path: "Limit", Name: "Limit", Location: bindstate.Location{Kind: "body", In: "limit"}, Extension: &spec.Parameter{WireSchema: &spec.WireSchema{Minimum: &minimum, Maximum: &maximum}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contract := testRouteContract(t, tc.typ, []bindly.BindingSpec{tc.binding})
			plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "constraints"}})
			require.NoError(t, err)
			property := plan.Metadata().InputSchema.Properties["limit"]
			require.Equal(t, "integer", property["type"])
			require.Equal(t, json.Number("1"), property["minimum"])
			require.Equal(t, json.Number("50"), property["maximum"])
			_, err = plan.Scope(map[string]any{"limit": nil})
			require.ErrorContains(t, err, "cannot be null")
			_, err = plan.Scope(map[string]any{})
			require.NoError(t, err)
			_, err = plan.Scope(map[string]any{"limit": 10})
			require.NoError(t, err)
		})
	}
}
