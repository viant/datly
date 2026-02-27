package column

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type sampleOrder struct {
	VendorID int    `sqlx:"name=VENDOR_ID"`
	Name     string `sqlx:"name=NAME"`
}

func TestUsesWildcard(t *testing.T) {
	tests := []struct {
		name string
		view *view.View
		want bool
	}{
		{name: "select wildcard", view: &view.View{Template: view.NewTemplate("SELECT * FROM VENDOR")}, want: true},
		{name: "select explicit", view: &view.View{Template: view.NewTemplate("SELECT ID, NAME FROM VENDOR")}, want: false},
		{name: "table only", view: &view.View{Table: "VENDOR"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, usesWildcard(tc.view))
		})
	}
}

func TestColumnsFromSchema_Order(t *testing.T) {
	aView := &view.View{Schema: state.NewSchema(reflect.TypeOf(sampleOrder{}), state.WithMany())}
	cols := columnsFromSchema(aView)
	require.Len(t, cols, 2)
	require.Equal(t, "VENDOR_ID", cols[0].Name)
	require.Equal(t, "NAME", cols[1].Name)
}

func TestMergePreservingOrder_AppendsNewDetectedColumns(t *testing.T) {
	base := view.Columns{
		view.NewColumn("VENDOR_ID", "int", reflect.TypeOf(int(0)), false),
		view.NewColumn("NAME", "varchar", reflect.TypeOf(""), false),
	}
	detected := view.Columns{
		view.NewColumn("NAME", "text", reflect.TypeOf(""), true),
		view.NewColumn("VENDOR_ID", "bigint", reflect.TypeOf(int64(0)), false),
		view.NewColumn("STATUS", "int", reflect.TypeOf(int(0)), true),
	}
	merged := mergePreservingOrder(base, detected)
	require.Len(t, merged, 3)
	require.Equal(t, "VENDOR_ID", merged[0].Name)
	require.Equal(t, "NAME", merged[1].Name)
	require.Equal(t, "STATUS", merged[2].Name)
	require.Equal(t, "bigint", merged[0].DataType)
	require.Equal(t, "text", merged[1].DataType)
}
