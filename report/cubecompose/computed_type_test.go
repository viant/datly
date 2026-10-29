package cubecompose

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputedScalarNativeScanner(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	for _, tc := range []struct {
		name, expression, source string
		typ                      reflect.Type
		want                     any
	}{
		{"exact integer", "t1.amount + 0", "SELECT 9007199254740993 AS amount", reflect.TypeFor[int64](), int64(9007199254740993)},
		{"exact sum", "SUM(t1.amount)", "SELECT 9007199254740993 AS amount", reflect.TypeFor[*int64](), int64(9007199254740993)},
		{"null sum", "SUM(t1.amount)", "SELECT NULL AS amount", reflect.TypeFor[*int64](), nil},
		{"zero count", "COUNT(t1.amount)", "SELECT NULL AS amount", reflect.TypeFor[int64](), int64(0)},
		{"null average", "AVG(t1.amount)", "SELECT NULL AS amount", reflect.TypeFor[*float64](), nil},
		{"fraction", "t1.amount / 2.0", "SELECT 3 AS amount", reflect.TypeFor[*float64](), float64(1.5)},
		{"zero divisor", "t1.amount / 0", "SELECT 3 AS amount", reflect.TypeFor[*float64](), nil},
		{"nullif", "NULLIF(t1.amount, 3)", "SELECT 3 AS amount", reflect.TypeFor[*int64](), nil},
		{"coalesce", "COALESCE(NULLIF(t1.amount, 3), 7)", "SELECT 3 AS amount", reflect.TypeFor[*int64](), int64(7)},
		{"unknown comparison", "NULLIF(t1.amount, 3) > 0", "SELECT 3 AS amount", reflect.TypeFor[*bool](), nil},
		{"true comparison", "t1.amount > 0", "SELECT 3 AS amount", reflect.TypeFor[*bool](), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, err := NewCatalog(Field{Name: "amount", Type: reflect.TypeFor[int64](), Role: Measure})
			require.NoError(t, err)
			plan, err := Compile("SELECT "+tc.expression+" AS value FROM $CubeSQL1 t1 LIMIT 1", catalog, 1, 1)
			require.NoError(t, err)
			require.Equal(t, tc.typ, plan.Columns[0].RuntimeType())
			query, args, err := plan.Render([]Frame{{SQL: tc.source}})
			require.NoError(t, err)
			dest := reflect.New(tc.typ)
			require.NoError(t, db.QueryRow(query, args...).Scan(dest.Interface()))
			value := dest.Elem()
			if value.Kind() == reflect.Pointer {
				if tc.want == nil {
					require.True(t, value.IsNil())
					return
				}
				require.False(t, value.IsNil())
				value = value.Elem()
			}
			require.Equal(t, tc.want, value.Interface())
		})
	}
	for _, expression := range []string{"NULL", "COALESCE(NULL, NULL)", "NULLIF(NULL, 0)", "secret(t1.amount)"} {
		_, err := Compile("SELECT "+expression+" AS value FROM $CubeSQL1 t1 LIMIT 1", caseCatalog(t), 1, 1)
		require.Error(t, err, expression)
	}
}
