package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	"github.com/xeipuuv/gojsonschema"
)

type primitivePairRow struct {
	IDs    []int64                             `json:"ids,omitempty" format:"inputNames=oldIds,inputNamesCase=fold"`
	Remove []int64                             `json:"removeIds,omitempty"`
	Hidden []int64                             `json:"privateIds" mcp:"-"`
	Has    *struct{ IDs, Remove, Hidden bool } `json:"-" setMarker:"true"`
}
type primitivePairBody struct {
	Selected []*primitivePairRow `json:"selected"`
	Other    []*primitivePairRow `json:"other"`
}
type primitivePairInput struct {
	Body *primitivePairBody `mcp:"name=payload,aliases=data"`
}

func TestPrimitivePairDiscoveryAndBinding(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeFor[primitivePairInput](), []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}}})
	exposure := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "update", PrimitivePairs: []spec.MCPPrimitivePair{{Path: "payload.selected", Add: "ids", Remove: "removeIds"}}}
	compile := func() *Plan {
		plan, err := NewCompiler(WithStrictArguments(true)).Compile(Input{Contract: contract, Exposure: exposure})
		require.NoError(t, err)
		return plan
	}
	plan := compile()
	fields := func(p *Plan, path string) map[string]any {
		props := schemaObjectNode(p.Metadata().InputSchema.Properties["payload"])["properties"].(map[string]any)
		return schemaObjectNode(props[path].(map[string]any))["properties"].(map[string]any)
	}
	for _, name := range []string{"idsToBeAdded", "idsToBeDeleted"} {
		require.Contains(t, fields(plan, "selected"), name)
	}
	for _, name := range []string{"ids", "oldIds", "removeIds"} {
		require.NotContains(t, fields(plan, "selected"), name)
		require.Contains(t, fields(plan, "other"), name)
	}
	require.NotContains(t, fields(plan, "other"), "idsToBeAdded")
	row := map[string]any{"idsToBeAdded": []any{json.Number("9223372036854775807")}, "idsToBeDeleted": []any{}}
	scope, err := plan.Scope(map[string]any{"data": map[string]any{"selected": []any{row}}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*primitivePairBody](), "")
	require.NoError(t, err)
	require.True(t, found)
	actual := value.(*primitivePairBody).Selected[0]
	require.Equal(t, []int64{9223372036854775807}, actual.IDs)
	require.True(t, actual.Has.IDs)
	require.True(t, actual.Has.Remove)
	require.NotContains(t, row, "ids")
	for _, invalid := range []map[string]any{
		{"idsToBeAdded": []int{}, "ids": []int{}},
		{"idsToBeAdded": []int{}, "OLDIDS": nil},
		{"idsToBeAdded": []int{3}, "removeIds": []any{float64(3)}},
		{"ids": []int{3}, "idsToBeDeleted": []any{json.Number("3")}},
		{"idsToBeDeleted": []any{1.5}},
		{"ids": []any{}}, {"oldIds": []any{}}, {"OLDIDS": nil}, {"removeIds": []any{}}, {"Remove": []any{}},
	} {
		_, err := plan.Scope(map[string]any{"payload": map[string]any{"selected": []any{invalid}}})
		require.Error(t, err)
	}
	walker, err := shape.CompileRewrite(plan.input.Type())
	require.NoError(t, err)
	repr, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[primitivePairRow]() && f.Name == "Remove" {
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	hidden, err := plan.EffectivePlan(&exec.EffectiveContract{Input: plan.input.Type(), InputRepresentation: repr})
	require.NoError(t, err)
	require.Empty(t, hidden.primitivePairs)
	require.NotContains(t, fields(hidden, "selected"), "idsToBeAdded")
	hiddenScope, err := hidden.Scope(map[string]any{"payload": map[string]any{"selected": []any{row}}})
	require.NoError(t, err)
	defer hiddenScope.Close()
	_, _, err = hiddenScope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*primitivePairBody](), "")
	require.Error(t, err)
	exposure.PrimitivePairs[0].Remove = "privateIds"
	require.NotContains(t, fields(compile(), "selected"), "idsToBeAdded")
	exposure.PrimitivePairs = nil
	require.NotContains(t, fields(compile(), "selected"), "idsToBeAdded")
}

func TestPrimitivePairCaseFormat(t *testing.T) {
	type row struct {
		MemberIds []int
		RemoveIds []int `json:"remove_EXACT"`
	}
	type input struct {
		RequestBody struct {
			ChildRows []row
			MemberIds []int
			RemoveIds []int `mcp:"exactRemove"`
		}
	}
	typ := reflect.TypeFor[input]()
	contract := testRouteContract(t, typ, []bindly.BindingSpec{{Path: "RequestBody", Location: state.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Name: "update", CaseFormat: "lu", Kind: spec.MCPExposureTool, PrimitivePairs: []spec.MCPPrimitivePair{{Path: "requestBody", Add: "memberIds", Remove: "exactRemove"}, {Path: "requestBody.childRows", Add: "memberIds", Remove: "remove_EXACT"}}}})
	require.NoError(t, err)
	props := schemaObjectNode(plan.Metadata().InputSchema.Properties["request_body"])["properties"].(map[string]any)
	children := schemaObjectNode(props["child_rows"].(map[string]any))["properties"].(map[string]any)
	require.NotContains(t, props, "exactRemove")
	require.NotContains(t, children, "remove_EXACT")
	for _, p := range []map[string]any{props, children} {
		require.Contains(t, p, "member_ids_to_be_added")
		require.Contains(t, p, "member_ids_to_be_deleted")
	}
	scope, err := plan.Scope(map[string]any{"request_body": map[string]any{"member_ids_to_be_added": []any{1}, "child_rows": []any{map[string]any{"member_ids_to_be_deleted": []any{2}}}}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), typ.Field(0).Type, "")
	require.NoError(t, err)
	require.True(t, found)
	body := reflect.ValueOf(value)
	require.Equal(t, []int{1}, body.FieldByName("MemberIds").Interface())
	require.Equal(t, []int{2}, body.FieldByName("ChildRows").Index(0).FieldByName("RemoveIds").Interface())
}

func TestPrimitivePairPreservesRequiredAliasGroup(t *testing.T) {
	type row struct {
		Ids    []int `json:"ids" required:"true" format:"inputNames=oldIds"`
		Remove []int `json:"removeIds,omitempty"`
	}
	type input struct{ Body *row }
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "update", PrimitivePairs: []spec.MCPPrimitivePair{{Path: "body", Add: "ids", Remove: "removeIds"}}}})
	require.NoError(t, err)
	for _, tc := range []struct {
		body  map[string]any
		valid bool
	}{
		{map[string]any{"ids": []int{1}}, false}, {map[string]any{"oldIds": []int{1}}, false}, {map[string]any{"idsToBeAdded": []int{1}}, true},
		{map[string]any{}, false}, {map[string]any{"idsToBeDeleted": []int{1}}, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(map[string]any{"body": tc.body}))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%v: %v", tc.body, result.Errors())
	}
}
