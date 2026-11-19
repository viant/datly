package view

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io"
	"github.com/viant/tagly/format/text"
)

func TestCollectorResolve_MapsProjectedAliasesToNativeTaggedFields(t *testing.T) {
	type siteView struct {
		SiteName *string `sqlx:"NAME"`
	}
	type publisherView struct {
		PublisherId   int    `sqlx:"ID"`
		PublisherName string `sqlx:"NAME"`
	}

	t.Run("SiteView", func(t *testing.T) {
		column := aliasTestColumn(t, reflect.TypeOf(siteView{}), "SiteName", "SITE_NAME", "NAME")
		aView := aliasTestView(column)
		collector := &Collector{view: aView}
		row := siteView{}

		target := collector.Resolve(io.NewColumn("SITE_NAME", "VARCHAR", reflect.TypeOf("")))(unsafe.Pointer(&row))
		value := "example.com"
		*target.(**string) = &value

		require.NotNil(t, row.SiteName)
		assert.Equal(t, value, *row.SiteName)
	})

	t.Run("PublisherView", func(t *testing.T) {
		idColumn := aliasTestColumn(t, reflect.TypeOf(publisherView{}), "PublisherId", "PUBLISHER_ID", "ID")
		nameColumn := aliasTestColumn(t, reflect.TypeOf(publisherView{}), "PublisherName", "PUBLISHER_NAME", "NAME")
		aView := aliasTestView(idColumn, nameColumn)
		collector := &Collector{view: aView}
		row := publisherView{}

		idTarget := collector.Resolve(io.NewColumn("PUBLISHER_ID", "INT", reflect.TypeOf(0)))(unsafe.Pointer(&row))
		nameTarget := collector.Resolve(io.NewColumn("PUBLISHER_NAME", "VARCHAR", reflect.TypeOf("")))(unsafe.Pointer(&row))
		*idTarget.(*int) = 17
		*nameTarget.(*string) = "publisher"

		assert.Equal(t, 17, row.PublisherId)
		assert.Equal(t, "publisher", row.PublisherName)
	})
}

func TestCollectorResolveViewColumn_IgnoresTransientAndNonAliasFields(t *testing.T) {
	type derivedView struct {
		Writable      bool    `sqlx:"-"`
		RunningBudget float64 `sqlx:"-"`
		Name          string  `sqlx:"NAME"`
	}
	rType := reflect.TypeOf(derivedView{})
	testCases := []struct {
		fieldName    string
		columnName   string
		databaseName string
	}{
		{fieldName: "Writable", columnName: "writable", databaseName: "writable"},
		{fieldName: "RunningBudget", columnName: "RUNNING_BUDGET", databaseName: "RUNNING_BUDGET"},
		{fieldName: "Name", columnName: "NAME", databaseName: "NAME"},
	}

	for _, testCase := range testCases {
		column := aliasTestColumn(t, rType, testCase.fieldName, testCase.columnName, testCase.databaseName)
		collector := &Collector{view: aliasTestView(column)}
		sourceColumn := io.NewColumn(testCase.columnName, "VARCHAR", reflect.TypeOf(""))

		assert.Nil(t, collector.resolveViewColumn(sourceColumn))
	}
}

func aliasTestColumn(t *testing.T, rType reflect.Type, fieldName, alias, nativeName string) *Column {
	t.Helper()
	field, ok := rType.FieldByName(fieldName)
	require.True(t, ok)
	column := &Column{Name: alias, DatabaseColumn: nativeName}
	column.SetField(field)
	return column
}

func aliasTestView(columns ...*Column) *View {
	aView := &View{Name: "alias_test", Columns: columns, CaseFormat: text.CaseFormatUpperUnderscore}
	aView._columns = Columns(columns).Index(aView.CaseFormat)
	return aView
}
