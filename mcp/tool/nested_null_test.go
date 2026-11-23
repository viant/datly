package tool

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
)

func TestAuthoredNestedNullAdmission(t *testing.T) {
	type row struct {
		ID       *int `json:"id" mcp:"name=orderId,aliases=oldOrderId" format:"nullable=false"`
		Optional *int `json:"optional"`
		Hidden   *int `json:"hidden" mcp:"-" format:"nullable=false"`
	}
	type body struct {
		Rows []*row `json:"rows"`
	}
	type named struct {
		Body *body
	}
	type flat struct {
		Body *body `anonymous:"true"`
	}
	for _, strict := range []bool{false, true} {
		for _, anonymous := range []bool{false, true} {
			t.Run(fmt.Sprintf("strict=%t/anonymous=%t", strict, anonymous), func(t *testing.T) {
				typ := reflect.TypeFor[named]()
				location := bindstate.Location{Kind: "body", In: "payload"}
				if anonymous {
					typ = reflect.TypeFor[flat]()
					location.In = ""
				}
				contract := testRouteContract(t, typ, []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: location}})
				plan, err := NewCompiler(WithStrictArguments(strict)).Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nested_null"}})
				require.NoError(t, err)
				for _, tc := range []struct {
					name      string
					rows      any
					wantError string
				}{
					{"canonical", []any{map[string]any{"orderId": nil}}, "orderId"},
					{"alias", []any{map[string]any{"oldOrderId": (*int)(nil)}}, "orderId"},
					{"raw null", []any{map[string]any{"id": json.RawMessage(" null ")}}, "orderId"},
					{"raw rows", json.RawMessage(`[{"id":null}]`), "orderId"},
					{"omitted", []any{map[string]any{}}, ""},
					{"zero", []any{map[string]any{"orderId": 0}}, ""},
					{"nullable", []any{map[string]any{"optional": nil}, nil}, ""},
					{"hidden and unknown", []any{map[string]any{"hidden": nil, "extra": nil}}, ""},
				} {
					t.Run(tc.name, func(t *testing.T) {
						args := map[string]any{"rows": tc.rows}
						if !anonymous {
							args = map[string]any{"body": args}
						}
						scope, err := plan.Scope(args)
						if tc.wantError != "" {
							require.ErrorAs(t, err, new(*bindinput.Error))
							require.ErrorContains(t, err, tc.wantError)
							require.ErrorContains(t, err, "cannot be null")
							return
						}
						require.NoError(t, err)
						defer scope.Close()
						locator := scope.Body().Locate(nil)
						decoded, found, err := locator.Value(t.Context(), reflect.TypeFor[*body](), location.In)
						if tc.name == "hidden and unknown" && strict {
							var invalid *bindinput.Error
							require.ErrorAs(t, err, &invalid)
							require.ErrorContains(t, invalid.Cause, "extra")
							return
						}
						require.NoError(t, err)
						require.True(t, found)
						if tc.name == "zero" {
							require.Equal(t, newInt(0), decoded.(*body).Rows[0].ID)
						}
						if tc.name == "omitted" {
							require.Nil(t, decoded.(*body).Rows[0].ID)
						}
					})
				}
			})
		}
	}
}

type opaqueNestedNullBody struct {
	ID *int `json:"id" format:"nullable=false"`
}

func (b *opaqueNestedNullBody) UnmarshalJSON(raw []byte) error {
	type payload opaqueNestedNullBody
	return json.Unmarshal(raw, (*payload)(b))
}

func TestNestedNullAdmissionFollowsEffectiveInput(t *testing.T) {
	type row struct {
		ID *int `json:"id"`
	}
	type body struct {
		Rows   map[string]*row       `json:"rows"`
		Opaque *opaqueNestedNullBody `json:"opaque"`
	}
	type input struct {
		Body *body
	}
	root := reflect.TypeFor[input]()
	contract := testRouteContract(t, root, []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nested_null_effective"}})
	require.NoError(t, err)
	walker, err := shape.CompileRewrite(root)
	require.NoError(t, err)
	args := map[string]any{"body": map[string]any{
		"rows":   map[string]any{"one": map[string]any{"LEGACYID": nil}},
		"opaque": map[string]any{"id": nil},
	}}
	for _, tc := range []struct {
		tag  reflect.StructTag
		fail bool
	}{
		{`json:"id" format:"nullable=true,inputNames=legacyId,inputNamesCase=fold"`, false},
		{`json:"id" format:"nullable=false,inputNames=legacyId,inputNamesCase=fold"`, true},
		{`json:"id" internal:"true" format:"nullable=false,inputNames=legacyId,inputNamesCase=fold"`, false},
		{`json:"id" format:"nullable=true,inputNames=legacyId,inputNamesCase=fold"`, false},
	} {
		representation, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
			if parent == reflect.TypeFor[row]() && field.Name == "ID" {
				return &shape.FieldMetadata{Name: field.Name, Tag: tc.tag}, nil
			}
			return nil, nil
		})
		require.NoError(t, err)
		selected, err := plan.EffectivePlan(&exec.EffectiveContract{Input: root, InputRepresentation: representation})
		require.NoError(t, err)
		scope, err := selected.Scope(args)
		if tc.fail {
			require.ErrorContains(t, err, `rows[\"one\"].id`)
		} else {
			require.NoError(t, err)
			require.NoError(t, scope.Close())
		}
	}
}

func TestNestedNullAdmissionUsesNativeInputSelection(t *testing.T) {
	type row struct {
		ID *int `json:"id" format:"nullable=false,inputNames=legacyId,inputNamesPriority=alias,inputNamesKinds=number|null,inputNamesCase=fold,inputUnless=../Ignore"`
	}
	type body struct {
		Ignore *bool `json:"ignore"`
		Rows   []row `json:"rows"`
	}
	type input struct {
		Body *body
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nested_null_selection"}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		guard bool
		row   map[string]any
		fail  bool
	}{
		{"discarded canonical", false, map[string]any{"id": nil, "LEGACYID": 0}, false},
		{"discarded alias kind", false, map[string]any{"id": 0, "LEGACYID": "ignored"}, false},
		{"chosen alias null", false, map[string]any{"id": 0, "LEGACYID": nil}, true},
		{"guarded field", true, map[string]any{"id": nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]any{"rows": []any{tc.row}}
			if tc.guard {
				payload["ignore"] = false
			}
			scope, err := plan.Scope(map[string]any{"body": payload})
			if tc.fail {
				require.ErrorContains(t, err, "rows[0].id")
				return
			}
			require.NoError(t, err)
			defer scope.Close()
			decoded, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*body](), "")
			require.NoError(t, err)
			require.True(t, found)
			if tc.guard {
				require.Nil(t, decoded.(*body).Rows[0].ID)
			} else {
				require.Equal(t, newInt(0), decoded.(*body).Rows[0].ID)
			}
		})
	}
}
