package load

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
)

type packageScopedPatchFoos struct {
	ID       int
	Name     *string
	Quantity *int
}

func TestComponent_InputReflectType_UsesComponentPackageForPatchHelpers(t *testing.T) {
	const pkgPath = "github.com/viant/datly/e2e/v1/shape/dev/generate_patch_basic_one"

	types := xreflect.NewTypes()
	require.NoError(t, types.Register("Foos",
		xreflect.WithPackage(pkgPath),
		xreflect.WithReflectType(reflect.TypeOf(packageScopedPatchFoos{})),
	))

	component := &Component{
		RootView: "Foos",
		TypeContext: &typectx.Context{
			PackagePath: pkgPath,
			PackageName: "generate_patch_basic_one",
		},
		Input: []*plan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewBodyLocation(""),
					Tag:    `anonymous:"true"`,
					Schema: &state.Schema{Name: "Foos"},
				},
			},
			{
				Parameter: state.Parameter{
					Name: "CurFoosId",
					In:   state.NewParameterLocation("Foos"),
					Tag:  `codec:"structql,uri=foos/cur_foos_id.sql"`,
					Schema: state.NewSchema(reflect.TypeOf(&struct {
						Values []int
					}{})),
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoos",
					In:     state.NewViewLocation("CurFoos"),
					Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
					Schema: &state.Schema{Name: "Foos", Cardinality: state.Many},
				},
			},
		},
	}

	rType, err := component.InputReflectType(pkgPath, types.Lookup, state.WithSetMarker(), state.WithTypeName("Input"))
	require.NoError(t, err)
	require.NotNil(t, rType)

	foosField, ok := rType.FieldByName("Foos")
	require.True(t, ok)
	assert.Equal(t, "packageScopedPatchFoos", namedType(foosField.Type).Name())

	curFoosField, ok := rType.FieldByName("CurFoos")
	require.True(t, ok)
	assert.Equal(t, reflect.Slice, curFoosField.Type.Kind())
	assert.Equal(t, "packageScopedPatchFoos", namedType(curFoosField.Type.Elem()).Name())

	curFoosIDField, ok := rType.FieldByName("CurFoosId")
	require.True(t, ok)
	assert.Equal(t, reflect.Ptr, curFoosIDField.Type.Kind())
	assert.Equal(t, reflect.Struct, curFoosIDField.Type.Elem().Kind())
	valuesField, ok := curFoosIDField.Type.Elem().FieldByName("Values")
	require.True(t, ok)
	assert.Equal(t, reflect.Slice, valuesField.Type.Kind())
	assert.Equal(t, reflect.Int, valuesField.Type.Elem().Kind())
}

func namedType(rType reflect.Type) reflect.Type {
	for rType != nil && (rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice) {
		rType = rType.Elem()
	}
	return rType
}
