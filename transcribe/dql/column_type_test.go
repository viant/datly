package dql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestColumnTypePointerSliceOrder(t *testing.T) {
	context := &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: "example.com/types"}}}
	for _, tc := range []struct {
		expression                  string
		pointer, many, slicePointer bool
	}{
		{"model.T", false, false, false},
		{"*model.T", true, false, false},
		{"[]model.T", false, true, false},
		{"[]*model.T", true, true, false},
		{"*[]model.T", false, true, true},
		{"([](*model.T))", true, true, false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			actual, err := ColumnType(tc.expression, context)
			require.NoError(t, err)
			require.Equal(t, "T", actual.Name)
			require.Equal(t, "example.com/types", actual.Package)
			require.Equal(t, tc.pointer, actual.Pointer)
			require.Equal(t, tc.many, actual.Cardinality == spec.CardinalityMany)
			require.Equal(t, tc.slicePointer, actual.SlicePointer)
			data, err := json.Marshal(actual)
			require.NoError(t, err)
			var decoded spec.TypeRef
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Equal(t, actual, decoded)
		})
	}
	for _, expression := range []string{"**model.T", "[][]model.T", "*[]*model.T", "[]**model.T", "[3]model.T", "map[string]model.T", "missing.T"} {
		t.Run("reject "+expression, func(t *testing.T) {
			_, err := ColumnType(expression, context)
			require.Error(t, err)
		})
	}
}

func TestDeclaredColumnTypePreservesPointerSliceOrder(t *testing.T) {
	component, err := parseComponentSource("example.com/app", "Shapes", `#import('model','example.com/types')
#setting($_ = $route('/shapes','GET'))
#define($_ = $Rows<[]Row>(view/rows).ColumnType('Values','[]*model.T').ColumnType('Pointer','*[]model.T') /* SELECT vals, ptr FROM shapes */)
SELECT 1`)
	require.NoError(t, err)
	require.Len(t, component.Views, 1)
	columns := component.Views[0].Columns
	require.Len(t, columns, 2)
	for _, column := range columns {
		column.Nullable = true
		require.Equal(t, column.Type, column.EffectiveType())
	}
	require.True(t, columns[0].Type.Pointer)
	require.False(t, columns[0].Type.SlicePointer)
	require.False(t, columns[1].Type.Pointer)
	require.True(t, columns[1].Type.SlicePointer)
}
