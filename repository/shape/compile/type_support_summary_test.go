package compile

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/x"
)

func TestApplyLinkedTypeSupport_RegistersSummaryTypes(t *testing.T) {
	source := &shape.Source{
		TypeRegistry: x.NewRegistry(),
	}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "meta_nested",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested",
		},
		Views: []*plan.View{
			{
				Name:        "vendor",
				SummaryName: "Meta",
				Summary:     "SELECT COUNT(*) AS CNT, 1 AS PAGE_CNT FROM ($View.vendor.SQL) t",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
			{
				Name:        "products",
				SummaryName: "ProductsMeta",
				Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) t GROUP BY VENDOR_ID",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
		},
	}

	applySummaryTypeSupport(result, source)
	applyLinkedTypeSupport(result, source)

	registry := source.EnsureTypeRegistry()
	require.NotNil(t, registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested.MetaView"))
	require.NotNil(t, registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested.ProductsMetaView"))

	var names []string
	for _, item := range result.Types {
		if item != nil {
			names = append(names, item.Name)
		}
	}
	assert.Contains(t, names, "MetaView")
	assert.Contains(t, names, "ProductsMetaView")
}

func TestApplyLinkedTypeSupport_SkipsPlaceholderLinkedViewTypes(t *testing.T) {
	type placeholderVendorView struct {
		Col1 string `sqlx:"name=col_1"`
		Col2 string `sqlx:"name=col_2"`
	}

	registry := x.NewRegistry()
	registry.Register(x.NewType(
		reflect.TypeOf(placeholderVendorView{}),
		x.WithName("VendorView"),
		x.WithPkgPath("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary"),
	))

	source := &shape.Source{TypeRegistry: registry}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "multi_summary",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary",
		},
		Views: []*plan.View{
			{
				Name:        "vendor",
				SchemaType:  "*VendorView",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
		},
	}

	applyLinkedTypeSupport(result, source)

	assert.Equal(t, reflect.TypeOf(map[string]interface{}{}), result.Views[0].ElementType)
	assert.Equal(t, reflect.TypeOf([]map[string]interface{}{}), result.Views[0].FieldType)
}

func TestApplySummaryTypeSupport_RegistersSummaryTypesWithoutLinkedViews(t *testing.T) {
	source := &shape.Source{
		TypeRegistry: x.NewRegistry(),
	}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "meta_nested",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested",
		},
		Views: []*plan.View{
			{
				Name:        "vendor",
				SummaryName: "Meta",
				Summary:     "SELECT COUNT(*) AS CNT, 1 AS PAGE_CNT FROM ($View.vendor.SQL) t",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
			{
				Name:        "products",
				SummaryName: "ProductsMeta",
				Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) t GROUP BY VENDOR_ID",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
		},
	}

	applySummaryTypeSupport(result, source)

	registry := source.EnsureTypeRegistry()
	require.NotNil(t, registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested.MetaView"))
	require.NotNil(t, registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/meta_nested.ProductsMetaView"))
}

func TestApplySummaryTypeSupport_PreservesOwnerColumnTypes(t *testing.T) {
	type productView struct {
		VendorId *int `sqlx:"VENDOR_ID"`
	}

	source := &shape.Source{TypeRegistry: x.NewRegistry()}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "multi_summary",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary",
		},
		Views: []*plan.View{
			{
				Name:        "products",
				SummaryName: "ProductsMeta",
				Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) t GROUP BY VENDOR_ID",
				FieldType:   reflect.TypeOf([]productView{}),
				ElementType: reflect.TypeOf(productView{}),
				Cardinality: "many",
			},
		},
	}

	applySummaryTypeSupport(result, source)

	registry := source.EnsureTypeRegistry()
	registered := registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary.ProductsMetaView")
	require.NotNil(t, registered)
	require.NotNil(t, registered.Type)
	field, ok := registered.Type.FieldByName("VendorId")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)
}

func TestApplySummaryTypeSupport_InfersNullableComputedSummaryColumns(t *testing.T) {
	source := &shape.Source{TypeRegistry: x.NewRegistry()}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "multi_summary",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary",
		},
		Views: []*plan.View{
			{
				Name:        "vendor",
				SummaryName: "Meta",
				Summary:     "SELECT CAST(1 + (COUNT(1) / 25) AS SIGNED) AS PAGE_CNT, COUNT(1) AS CNT FROM ($View.vendor.SQL) t",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
				Cardinality: "many",
			},
		},
	}

	applySummaryTypeSupport(result, source)

	registry := source.EnsureTypeRegistry()
	registered := registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary.MetaView")
	require.NotNil(t, registered)
	require.NotNil(t, registered.Type)
	pageCnt, ok := registered.Type.FieldByName("PageCnt")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), pageCnt.Type)
	cnt, ok := registered.Type.FieldByName("Cnt")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf(int(0)), cnt.Type)
}

func TestApplySummaryTypeSupport_OverridesStaleRegisteredSummaryType(t *testing.T) {
	type staleProductsMetaView struct {
		VendorId string `sqlx:"VENDOR_ID"`
	}
	type productView struct {
		VendorId *int `sqlx:"VENDOR_ID"`
	}

	registry := x.NewRegistry()
	registry.Register(x.NewType(
		reflect.TypeOf(staleProductsMetaView{}),
		x.WithName("ProductsMetaView"),
		x.WithPkgPath("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary"),
	))

	source := &shape.Source{TypeRegistry: registry}
	result := &plan.Result{
		TypeContext: &typectx.Context{
			PackageName: "multi_summary",
			PackagePath: "github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary",
		},
		Views: []*plan.View{
			{
				Name:        "products",
				SummaryName: "ProductsMeta",
				Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) t GROUP BY VENDOR_ID",
				FieldType:   reflect.TypeOf([]productView{}),
				ElementType: reflect.TypeOf(productView{}),
				Cardinality: "many",
			},
		},
	}

	applySummaryTypeSupport(result, source)

	registered := registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary.ProductsMetaView")
	require.NotNil(t, registered)
	require.NotNil(t, registered.Type)
	field, ok := registered.Type.FieldByName("VendorId")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)
}
