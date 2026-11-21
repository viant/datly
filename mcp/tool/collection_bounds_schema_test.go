package tool

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

func TestCollectionBoundsWireSchemaProjection(t *testing.T) {
	type input struct {
		Names []string `json:"names"`
	}
	bounds := &spec.WireSchema{MinItems: 1, MaxItems: 2}
	raw, err := json.Marshal(bounds)
	require.NoError(t, err)
	var restored spec.WireSchema
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, bounds, restored.Clone())
	for _, tc := range []struct {
		name     string
		schema   map[string]any
		payloads []string
	}{
		{"nested", mustCollectionBoundsProjection(t, reflect.TypeFor[input](), bounds), []string{`{"names":[]}`, `{"names":["a","b"]}`, `{"names":["a","b","c"]}`}},
		{"root", wireSchemaMap(&spec.WireSchema{Type: "array", MinItems: 1, MaxItems: 2}), []string{`[]`, `["a","b"]`, `["a","b","c"]`}},
		{"alternative", wireSchemaMap(&spec.WireSchema{AnyOf: []*spec.WireSchema{{Type: "array", MinItems: 1, MaxItems: 2}, {Type: "string"}}}), []string{`[]`, `["a","b"]`, `["a","b","c"]`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(tc.schema))
			require.NoError(t, err)
			for i, payload := range tc.payloads {
				result, err := validator.Validate(gojsonschema.NewStringLoader(payload))
				require.NoError(t, err)
				require.Equal(t, i == 1, result.Valid(), "%s: %v", payload, result.Errors())
			}
		})
	}
}

func mustCollectionBoundsProjection(t *testing.T, typ reflect.Type, bounds *spec.WireSchema) map[string]any {
	t.Helper()
	projected, err := (&schemaProjector{schemas: map[string]*spec.WireSchema{"Body.names": bounds}}).argument(typ, "Body", "Body", nil)
	require.NoError(t, err)
	properties := projected["properties"].(map[string]any)
	names := properties["names"].(map[string]any)
	require.NotNil(t, names["items"], "bounds must retain reflected array items")
	return projected
}

func TestCollectionBoundsCompiledToolSchema(t *testing.T) {
	type deal struct {
		ID int `json:"id"`
	}
	type input struct {
		PMPDeals []deal `json:"pmp_deals" parameter:"PMPDeals,kind=body,in=pmp_deals" wireSchema:"{\"type\":\"array\",\"minItems\":1,\"maxItems\":50}"`
	}
	field, _ := reflect.TypeFor[input]().FieldByName("PMPDeals")
	var bounds spec.WireSchema
	require.NoError(t, json.Unmarshal([]byte(field.Tag.Get("wireSchema")), &bounds))
	binding := bindly.BindingSpec{Path: "PMPDeals", Name: "PMPDeals", Location: bindstate.Location{Kind: "body", In: "pmp_deals"}, Extension: &spec.Parameter{WireSchema: &bounds}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "batch"}})
	require.NoError(t, err)
	schema := plan.Metadata().InputSchema
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"maxItems":50`)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(raw))
	require.NoError(t, err)
	for _, count := range []int{0, 1, 50, 51} {
		rows := make([]map[string]any, count)
		for i := range rows {
			rows[i] = map[string]any{"id": i}
		}
		payload, err := json.Marshal(map[string]any{"pmp_deals": rows})
		require.NoError(t, err)
		result, err := validator.Validate(gojsonschema.NewBytesLoader(payload))
		require.NoError(t, err)
		require.Equal(t, count > 0 && count <= 50, result.Valid(), "%s: %v", payload, result.Errors())
	}
	result, err := validator.Validate(gojsonschema.NewStringLoader(`{"pmp_deals":null}`))
	require.NoError(t, err)
	require.False(t, result.Valid())
	bounds.Nullable = true
	nullableContract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	nullablePlan, err := NewCompiler().Compile(Input{Contract: nullableContract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nullable_batch"}})
	require.NoError(t, err)
	nullableRaw, err := json.Marshal(nullablePlan.Metadata().InputSchema)
	require.NoError(t, err)
	nullableResult, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(nullableRaw), gojsonschema.NewStringLoader(`{"pmp_deals":null}`))
	require.NoError(t, err)
	require.True(t, nullableResult.Valid(), "%s: %v", nullableRaw, nullableResult.Errors())
	prop := schema.Properties["pmp_deals"]
	require.NotNil(t, prop["items"])
}

func TestStrictSuppliedQueryArrayBounds(t *testing.T) {
	type input struct {
		Names []string `parameter:"Names,kind=query,in=names" mcp:"name=names,aliases=legacyNames"`
	}
	for _, strict := range []bool{true, false} {
		for _, nullable := range []bool{true, false} {
			bounds := &spec.WireSchema{Type: "array", MinItems: 1, MaxItems: 50, Nullable: nullable}
			binding := bindly.BindingSpec{Path: "Names", Name: "Names", Location: bindstate.Location{Kind: "query", In: "names"}, Extension: &spec.Parameter{WireSchema: bounds}}
			contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
			plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Names"}, Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "names", StrictArguments: &strict}})
			require.NoError(t, err)
			name := plan.args[0].publicName
			for _, tc := range []struct {
				label  string
				args   map[string]any
				reject bool
			}{
				{"omitted", nil, false},
				{"null", map[string]any{name: nil}, !nullable},
				{"encoded null", map[string]any{name: json.RawMessage("null")}, !nullable},
				{"empty", map[string]any{name: []string{}}, true},
				{"empty alias", map[string]any{"legacyNames": []any{}}, true},
				{"scalar", map[string]any{name: "a,b"}, false},
				{"one", map[string]any{name: []any{"a,b"}}, false},
				{"fifty", map[string]any{name: make([]string, 50)}, false},
				{"fifty-one", map[string]any{name: make([]string, 51)}, true},
			} {
				calls := 0
				handler := NewHandler(plan, invocation.New(invocation.Config{Invoker: binaryComponentFunc(func(context.Context, exec.ComponentRequest) (any, error) {
					calls++
					return nil, nil
				})}))
				req := &schema.CallToolRequest{}
				req.Params.Name = plan.Metadata().Name
				req.Params.Arguments = tc.args
				_, rpc := handler.Handle(t.Context(), req)
				if strict && tc.reject {
					require.NotNil(t, rpc, tc.label)
					require.EqualValues(t, -32602, rpc.Code, tc.label)
					require.Contains(t, rpc.Message, name)
					require.Zero(t, calls, tc.label)
				} else {
					require.Nil(t, rpc, tc.label)
					require.Equal(t, 1, calls, tc.label)
				}
			}
		}
	}
}
