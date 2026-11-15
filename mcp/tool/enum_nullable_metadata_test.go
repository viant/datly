package tool

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	"github.com/xeipuuv/gojsonschema"
)

func TestEnumNullableMetadataQuerySchema(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		for _, format := range []string{"", "nullable=true", "nullable=false"} {
			for _, reject := range []bool{false, true} {
				t.Run(fmt.Sprintf("pointer=%t/format=%s/reject=%t", pointer, format, reject), func(t *testing.T) {
					typ := reflect.TypeFor[string]()
					if pointer {
						typ = reflect.PointerTo(typ)
					}
					tag := reflect.StructTag(`parameter:",kind=query,in=mode,required=false,value=ONE" choice:"ONE" choice:"TWO" enumAccept:"literal" enumStyle:"literal"`)
					if format != "" {
						tag += reflect.StructTag(` format:"` + format + `"`)
					}
					if reject {
						tag += ` enumNull:"reject"`
					}
					root := reflect.StructOf([]reflect.StructField{{Name: "Mode", Type: typ, Tag: tag}})
					field := root.Field(0)
					binding, _, err := bindly.BindingSpecFromField(field)
					require.NoError(t, err)
					binding, err = registry.EnumBinding(binding, field)
					require.NoError(t, err)
					contract := testRouteContract(t, root, []bindly.BindingSpec{binding})
					plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nullable_enum"}})
					require.NoError(t, err)
					nullable := pointer
					if format != "" {
						nullable = format == "nullable=true"
					}
					nullable = nullable && !reject
					walker, err := shape.CompileRewrite(root)
					require.NoError(t, err)
					metadata, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
						return &shape.FieldMetadata{Name: f.Name, Tag: f.Tag + ` desc:"metadata probe"`}, nil
					})
					require.NoError(t, err)
					fields, err := contract.MetadataRepresentation(metadata, nil)
					require.NoError(t, err)
					require.Equal(t, nullable, fields[0].WireSchema().Nullable, "registry enum wire metadata follows existing format policy")

					for _, value := range []any{nil, "ONE", "TWO", "bad", true, 1.5} {
						args := map[string]any{"mode": value}
						result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
						require.NoError(t, err)
						valid := value == "ONE" || value == "TWO" || value == nil && nullable
						require.Equal(t, valid, result.Valid(), "value=%v errors=%v", value, result.Errors())
					}
					// Formatting adjusts the schema only; query null still follows native enum
					// rejection (when declared) and otherwise omission before default binding.
					scope, err := plan.Scope(map[string]any{"mode": nil})
					if reject {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						_, present, err := scope.Query().Locate(nil).Value(t.Context(), reflect.TypeFor[string](), "mode")
						require.NoError(t, err)
						require.False(t, present)
						require.NoError(t, scope.Close())
					}
				})
			}
		}
	}
}

type nullableMetadataAliasRow struct {
	Mode string `json:"mode" choice:"0:ONE" choice:"1:TWO" enumAccept:"scalar" enumStyle:"literal" enumAlias:"1:Second"`
}
type nullableMetadataAliasInput struct {
	Body *nullableMetadataAliasRow `parameter:"Body,kind=body"`
}

func TestEnumNullableMetadataNestedEffectiveRepresentation(t *testing.T) {
	root := reflect.TypeFor[nullableMetadataAliasInput]()
	contract := testRouteContract(t, root, []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nullable_alias"}})
	require.NoError(t, err)
	walker, err := shape.CompileRewrite(root)
	require.NoError(t, err)
	for _, nullable := range []bool{true, false, true} {
		representation, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
			if parent == reflect.TypeFor[nullableMetadataAliasRow]() && field.Name == "Mode" {
				return &shape.FieldMetadata{Name: field.Name, Tag: field.Tag + reflect.StructTag(fmt.Sprintf(` format:"nullable=%t"`, nullable))}, nil
			}
			return nil, nil
		})
		require.NoError(t, err)
		selected, err := plan.EffectivePlan(&exec.EffectiveContract{Input: root, InputRepresentation: representation})
		require.NoError(t, err)
		for _, value := range []any{nil, "Second", 1, "unknown", false} {
			args := map[string]any{"body": map[string]any{"mode": value}}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(selected.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			require.NoError(t, err)
			want := value == "Second" || value == 1 || value == nil && nullable
			require.Equal(t, want, result.Valid(), "nullable=%t value=%v errors=%v", nullable, value, result.Errors())
		}
		// Existing alias conversion and canonical storage are retained by native binding.
		providers, err := selected.Scope(map[string]any{"body": map[string]any{"mode": "Second"}})
		require.NoError(t, err)
		execution, err := contract.WithInputRepresentation(representation)
		require.NoError(t, err)
		injector, err := bindly.NewInjector()
		require.NoError(t, err)
		scope, err := injector.ForScope(providers.Providers()...)
		require.NoError(t, err)
		var actual nullableMetadataAliasInput
		require.NoError(t, scope.Bind(t.Context(), &actual, bindly.WithPlan(execution.Plan())))
		require.Equal(t, "TWO", actual.Body.Mode)
		require.NoError(t, providers.Close())
	}
}
