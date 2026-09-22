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

type sampleSemanticRoot struct {
	ID       int                `sqlx:"ID"`
	Products []*sampleChildView `view:",table=PRODUCT" on:"Id:ID=VendorId:VENDOR_ID" sql:"uri=vendor/products.sql"`
	Cities   []*sampleChildView `view:",table=CITY" on:"Id:ID=DistrictId:DISTRICT_ID"`
	Ignored  string             `sqlx:"-"`
}

type sampleChildView struct {
	VendorID int `sqlx:"VENDOR_ID"`
}

func stringPtr(value string) *string {
	return &value
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

func TestColumnsFromSchema_SkipsSemanticRelationFields(t *testing.T) {
	aView := &view.View{Schema: state.NewSchema(reflect.TypeOf(sampleSemanticRoot{}), state.WithMany())}
	cols := columnsFromSchema(aView)
	require.Len(t, cols, 1)
	require.Equal(t, "ID", cols[0].Name)
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

func TestApplyConstValuesForDiscovery(t *testing.T) {
	resource := view.EmptyResource()
	resource.AddParameters(
		&state.Parameter{Name: "Vendor", In: state.NewConstLocation("Vendor"), Value: "VENDOR"},
		&state.Parameter{Name: "Product", In: state.NewConstLocation("Product"), Value: "PRODUCT"},
	)
	sql := `SELECT vendor.*, products.* FROM (SELECT * FROM $Vendor t) vendor JOIN (SELECT * FROM ${Unsafe.Product} p) products ON products.VENDOR_ID = vendor.ID`
	got := applyConstValuesForDiscovery(sql, resource)
	require.Contains(t, got, "FROM (SELECT * FROM VENDOR t)")
	require.Contains(t, got, "JOIN (SELECT * FROM PRODUCT p)")
	require.NotContains(t, got, "$Vendor")
	require.NotContains(t, got, "${Unsafe.Product}")
}

func TestDiscoverySQL_ResolvesConstTableFallback(t *testing.T) {
	resource := view.EmptyResource()
	resource.AddParameters(
		&state.Parameter{Name: "Vendor", In: state.NewConstLocation("Vendor"), Value: "VENDOR"},
	)
	aView := &view.View{
		Table:    "${Unsafe.Vendor}",
		Template: view.NewTemplate("SELECT * FROM ${Unsafe.Vendor} t WHERE t.ID = $criteria.AppendBinding($Unsafe.VendorID)"),
	}
	got := discoverySQL(aView, resource)
	require.Equal(t, "VENDOR", got)
}

func TestNormalizeDiscoveryTable_TemplateUnsafe(t *testing.T) {
	require.Equal(t, "Vendor", normalizeDiscoveryTable("${Unsafe.Vendor}"))
	require.Equal(t, "Product", normalizeDiscoveryTable("$Unsafe.Product"))
}

func TestDiscoverySQL_TemplateTableWithoutConstParameter_UsesNormalizedTable(t *testing.T) {
	aView := &view.View{
		Table:    "${Unsafe.Vendor}",
		Template: view.NewTemplate("SELECT * FROM ${Unsafe.Vendor} t WHERE t.ID IN ($criteria.AppendBinding($Unsafe.vendorIDs))"),
	}
	got := discoverySQL(aView, view.EmptyResource())
	require.Equal(t, "Vendor", got)
}

func TestExplicitProjectedSubqueryColumns_UsesInnerProjection(t *testing.T) {
	aView := &view.View{
		Template: view.NewTemplate("SELECT * FROM (SELECT (1) AS IS_ACTIVE, (3) AS CHANNEL, CAST($criteria.AppendBinding($Unsafe.VendorID) AS SIGNED) AS ID) t"),
		ColumnsConfig: map[string]*view.ColumnConfig{
			"ID": {Name: "ID", Tag: stringPtr(`internal:"true"`)},
		},
	}
	got := explicitProjectedSubqueryColumns(aView)
	require.Len(t, got, 3)
	require.Equal(t, "IS_ACTIVE", got[0].Name)
	require.Equal(t, "int", got[0].DataType)
	require.Equal(t, "CHANNEL", got[1].Name)
	require.Equal(t, "int", got[1].DataType)
	require.Equal(t, "ID", got[2].Name)
	require.Equal(t, ` internal:"true"`, got[2].Tag)
}

func TestInferDiscoveryTable(t *testing.T) {
	require.Equal(t, "Vendor", inferDiscoveryTable("SELECT * FROM ${Unsafe.Vendor} t WHERE 1=1"))
	require.Equal(t, "Product", inferDiscoveryTable("SELECT * FROM $Unsafe.Product t WHERE 1=1"))
	require.Equal(t, "VENDOR", inferDiscoveryTable("SELECT * FROM VENDOR t WHERE 1=1"))
	require.Equal(t, "Vendor", inferDiscoveryTable("SELECT vendor.* FROM (SELECT * FROM ${Unsafe.Vendor} t WHERE 1=1) vendor"))
}
