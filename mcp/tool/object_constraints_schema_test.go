package tool

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type constraintRow struct {
	ID     *int    `json:"id,omitempty" format:"nullable=false"`
	Name   *string `json:"name,omitempty" format:"nullable=false"`
	Owner  *int    `json:"owner,omitempty" format:"nullable=false"`
	Note   *string `json:"note,omitempty"`
	Hidden *string `json:"hidden,omitempty" mcp:"-"`
}
type constraintBody struct {
	Rows []*constraintRow `json:"rows" required:"true" format:"nullable=false"`
}

func rowConstraints() *spec.WireSchema {
	closed := false
	return &spec.WireSchema{Required: true, MinItems: 1, MaxItems: 50, Items: &spec.WireSchema{Required: true, AdditionalProperties: &closed, AnyOf: []*spec.WireSchema{{RequiredFields: []string{"id"}}, {RequiredFields: []string{"name", "owner"}}}}}
}

func TestObjectConstraintsNativeNamedAndAnonymousDiscovery(t *testing.T) {
	type named struct{ Body *constraintBody }
	type anonymous struct {
		Body *constraintBody `anonymous:"true"`
	}
	for _, tc := range []struct {
		name      string
		typ       reflect.Type
		anonymous bool
	}{{"named", reflect.TypeFor[named](), false}, {"anonymous", reflect.TypeFor[anonymous](), true}} {
		t.Run(tc.name, func(t *testing.T) {
			policy := rowConstraints()
			raw, err := json.Marshal(policy)
			require.NoError(t, err)
			var restored spec.WireSchema
			require.NoError(t, json.Unmarshal(raw, &restored))
			clone := restored.Clone()
			require.Equal(t, policy, clone)
			clone.Items.RequiredFields = []string{"changed"}
			*clone.Items.AdditionalProperties = true
			require.False(t, *policy.Items.AdditionalProperties)
			binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}, Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Body.Rows": policy}}}
			contract := testRouteContract(t, tc.typ, []bindly.BindingSpec{binding})
			snapshot := contract.Fields()[0].WireSchemas()
			snapshot["Body.Rows"].Items.AnyOf[0].RequiredFields[0] = "mutated"
			require.Equal(t, "id", contract.Fields()[0].WireSchemas()["Body.Rows"].Items.AnyOf[0].RequiredFields[0])
			plan, err := NewCompiler(WithStrictArguments(true)).Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "save"}, Contract: contract})
			require.NoError(t, err)
			schema := plan.Metadata().InputSchema
			rawSchema, _ := json.Marshal(schema)
			t.Log(string(rawSchema))
			if !tc.anonymous {
				schema.Properties = map[string]map[string]any{"rows": schema.Properties["body"]["properties"].(map[string]any)["rows"].(map[string]any)}
				schema.Required = []string{"rows"}
			}
			validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schema))
			require.NoError(t, err)
			for _, sample := range []struct {
				raw   string
				valid bool
			}{
				{`{}`, false}, {`{"rows":null}`, false}, {`{"rows":[]}`, false}, {`{"rows":[null]}`, false},
				{`{"rows":[{}]}`, false}, {`{"rows":[{"id":1}]}`, true}, {`{"rows":[{"name":"N","owner":2}]}`, true},
				{`{"rows":[{"name":"N"}]}`, false}, {`{"rows":[{"id":null}]}`, false}, {`{"rows":[{"id":1,"name":null}]}`, false},
				{`{"rows":[{"id":1,"note":null}]}`, true}, {`{"rows":[{"id":1,"hidden":"secret"}]}`, false}, {`{"rows":[{"id":1,"unknown":true}]}`, false},
			} {
				result, err := validator.Validate(gojsonschema.NewStringLoader(sample.raw))
				require.NoError(t, err)
				require.Equal(t, sample.valid, result.Valid(), "%s: %v", sample.raw, result.Errors())
			}
			for _, count := range []int{50, 51} {
				rows := make([]any, count)
				for i := range rows {
					rows[i] = map[string]any{"id": i + 1}
				}
				result, err := validator.Validate(gojsonschema.NewGoLoader(map[string]any{"rows": rows}))
				require.NoError(t, err)
				require.Equal(t, count == 50, result.Valid())
			}
		})
	}
}

func TestObjectConstraintsKeepCompleteVisibleAlternatives(t *testing.T) {
	policy := rowConstraints()
	policy.Items.AnyOf[1].RequiredFields = append(policy.Items.AnyOf[1].RequiredFields, "hidden")
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.Rows": policy}}).argument(reflect.TypeFor[constraintBody](), "Body", "body", nil)
	require.NoError(t, err)
	rawSchema, _ := json.Marshal(projected)
	t.Log(string(rawSchema))
	rows := projected["properties"].(map[string]any)["rows"].(map[string]any)
	item := rows["items"].(map[string]any)
	branches := item["anyOf"].([]any)
	require.Len(t, branches, 1)
	require.Equal(t, []string{"id"}, branches[0].(map[string]any)["required"])
	policy.Items.AnyOf = policy.Items.AnyOf[1:]
	_, err = (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.Rows": policy}}).argument(reflect.TypeFor[constraintBody](), "Body", "body", nil)
	require.ErrorContains(t, err, "no available required-field alternative")
}

func TestTypedItemConstraintsKeepReflectedArrayRoot(t *testing.T) {
	policy := &spec.WireSchema{MinItems: 1, MaxItems: 2, Items: &spec.WireSchema{Type: "integer"}}
	projected, err := (&schemaProjector{}).argument(reflect.TypeFor[[]int](), "Rows", "rows", policy)
	require.NoError(t, err)
	require.Equal(t, "array", projected["type"], "type-empty metadata must retain the reflected array root")
	require.Equal(t, "integer", projected["items"].(map[string]any)["type"])
	require.True(t, hasWireRepresentation(map[string]*spec.WireSchema{"Rows": policy}), "typed nested items retain existing explicit-representation naming guard")
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, sample := range []struct {
		raw   string
		valid bool
	}{{`[1]`, true}, {`1`, false}, {`[]`, false}, {`[1,2,3]`, false}, {`["one"]`, false}} {
		result, err := validator.Validate(gojsonschema.NewStringLoader(sample.raw))
		require.NoError(t, err)
		require.Equal(t, sample.valid, result.Valid(), "%s: %v", sample.raw, result.Errors())
	}
}
