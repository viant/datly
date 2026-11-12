package xgen

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestRebuildResourceViewStructType_ReplacesPlaceholderColumnsPreservesRelations(t *testing.T) {
	type placeholderProducts struct {
		ID int `sqlx:"ID"`
	}
	type placeholderVendor struct {
		Col1     string                 `sqlx:"name=col_1"`
		Col2     string                 `sqlx:"name=col_2"`
		Products []*placeholderProducts `view:",table=PRODUCT" sql:"uri=vendor/products.sql" sqlx:"-"`
	}

	cols := []columnDescriptor{
		{name: "ID", dataType: "int", primaryKey: true},
		{name: "NAME", dataType: "string"},
	}

	rType := rebuildResourceViewStructType(reflect.TypeOf(placeholderVendor{}), cols, false)
	require.NotNil(t, rType)
	require.Equal(t, reflect.Struct, rType.Kind())

	field, ok := rType.FieldByName("Id")
	require.True(t, ok)
	require.Equal(t, "ID", sqlxTagName(field.Tag.Get("sqlx")))

	field, ok = rType.FieldByName("Name")
	require.True(t, ok)
	require.Equal(t, "NAME", sqlxTagName(field.Tag.Get("sqlx")))

	_, ok = rType.FieldByName("Col1")
	require.False(t, ok)

	field, ok = rType.FieldByName("Products")
	require.True(t, ok)
	require.Equal(t, `uri=vendor/products.sql`, field.Tag.Get("sql"))
	require.Equal(t, ",table=PRODUCT", field.Tag.Get("view"))
}

func TestComponentCodegen_UsesDiscoveredColumnsWhenRootTypeIsPlaceholder(t *testing.T) {
	type placeholderProducts struct {
		ID int `sqlx:"ID"`
	}
	type placeholderVendor struct {
		Col1     string                 `sqlx:"name=col_1"`
		Products []*placeholderProducts `view:",table=PRODUCT" sql:"uri=vendor/products.sql" sqlx:"-"`
	}

	resource := view.EmptyResource()
	resource.Views = view.Views{
		{
			Name: "vendor",
			Schema: &state.Schema{
				Name: "VendorView",
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string"},
			},
		},
	}
	resource.Views[0].Schema.SetType(reflect.TypeOf([]placeholderVendor{}))

	codegen := &ComponentCodegen{Resource: resource}
	rType := codegen.resourceViewStructType("vendor")
	require.NotNil(t, rType)
	_, ok := rType.FieldByName("Id")
	require.True(t, ok)
	_, ok = rType.FieldByName("Name")
	require.True(t, ok)
	_, ok = rType.FieldByName("Products")
	require.True(t, ok)
	_, ok = rType.FieldByName("Col1")
	require.False(t, ok)
}
