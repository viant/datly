package scan

import (
	"context"
	"embed"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/x"
	"github.com/viant/xdatly"
)

//go:embed testdata/*.sql
var testFS embed.FS

type embeddedFS struct{}

func (embeddedFS) EmbedFS() *embed.FS {
	return &testFS
}

type reportRow struct {
	ID   int
	Name string
}

type reportSource struct {
	embeddedFS
	Rows  []reportRow `view:"rows,table=REPORT,connector=dev,type=ReportRow,dest=rows.go" sql:"uri=testdata/report.sql"`
	ID    int         `parameter:"id,kind=query,in=id"`
	Route struct{}    `component:",path=/v1/api/dev/report,method=GET,connector=dev"`
}

type reportInput struct {
	ID int
}

type reportOutput struct {
	Data []reportRow
}

type typedComponentSource struct {
	Route xdatly.Component[reportInput, reportOutput] `component:",path=/v1/api/dev/report,method=GET"`
}

type reportEnabledSource struct {
	Route xdatly.Component[reportInput, reportOutput] `component:",path=/v1/api/dev/report,method=GET,report=true,reportInput=NamedReportInput,reportDimensions=Dims,reportMeasures=Metrics,reportFilters=Predicates,reportOrderBy=Sort,reportLimit=Take,reportOffset=Skip"`
}

type dynamicReportInput struct {
	Name string
}

type dynamicReportOutput struct {
	Count int
}

type namedReportInput struct {
	Name string `parameter:"name,kind=query,in=name"`
}

type namedReportOutput struct {
	Data []reportRow `parameter:"data,kind=output,in=view"`
}

type selectorHolderSource struct {
	Route      xdatly.Component[reportInput, reportOutput] `component:",path=/v1/api/dev/report,method=GET"`
	ViewSelect struct {
		Fields []string `parameter:"fields,kind=query,in=_fields"`
		Page   int      `parameter:"page,kind=query,in=_page"`
	} `querySelector:"rows"`
}

func TestStructScanner_Scan(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)
	require.NotNil(t, result)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.NotNil(t, descriptors)
	require.NotNil(t, descriptors.EmbedFS)
	assert.Equal(t, reflect.TypeOf(reportSource{}), descriptors.RootType)

	rows := descriptors.ByPath["Rows"]
	require.NotNil(t, rows)
	require.True(t, rows.HasViewTag)
	require.NotNil(t, rows.ViewTag)
	assert.Equal(t, "rows", rows.ViewTag.View.Name)
	assert.Equal(t, "ReportRow", rows.ViewTypeName)
	assert.Equal(t, "rows.go", rows.ViewDest)
	assert.Contains(t, rows.ViewTag.SQL.SQL, "SELECT ID, NAME FROM REPORT")

	idField := descriptors.ByPath["ID"]
	require.NotNil(t, idField)
	require.True(t, idField.HasStateTag)
	require.NotNil(t, idField.StateTag)
	require.NotNil(t, idField.StateTag.Parameter)
	assert.Equal(t, "id", idField.StateTag.Parameter.Name)
	assert.Equal(t, "query", idField.StateTag.Parameter.Kind)
	assert.Equal(t, "id", idField.StateTag.Parameter.In)

	route := descriptors.ByPath["Route"]
	require.NotNil(t, route)
	require.True(t, route.HasComponentTag)
	require.NotNil(t, route.ComponentTag)
	require.NotNil(t, route.ComponentTag.Component)
	assert.Equal(t, "/v1/api/dev/report", route.ComponentTag.Component.Path)
	assert.Equal(t, "GET", route.ComponentTag.Component.Method)
	assert.Equal(t, "dev", route.ComponentTag.Component.Connector)
}

func TestStructScanner_Scan_InvalidSource(t *testing.T) {
	scanner := New()
	_, err := scanner.Scan(context.Background(), &shape.Source{Struct: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct")
}

func TestStructScanner_Scan_ComponentHolderTypes(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &typedComponentSource{}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.Len(t, descriptors.ComponentFields, 1)
	route := descriptors.ComponentFields[0]
	require.NotNil(t, route)
	assert.Equal(t, reflect.TypeOf(reportInput{}), route.ComponentInputType)
	assert.Equal(t, reflect.TypeOf(reportOutput{}), route.ComponentOutputType)
	assert.Empty(t, route.ComponentInputName)
	assert.Empty(t, route.ComponentOutputName)
}

func TestStructScanner_Scan_ComponentReportTags(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportEnabledSource{}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.Len(t, descriptors.ComponentFields, 1)
	route := descriptors.ComponentFields[0]
	require.NotNil(t, route)
	require.NotNil(t, route.ComponentTag)
	require.NotNil(t, route.ComponentTag.Component)
	assert.True(t, route.ComponentTag.Component.Report)
	assert.Equal(t, "NamedReportInput", route.ComponentTag.Component.ReportInput)
	assert.Equal(t, "Dims", route.ComponentTag.Component.ReportDimensions)
	assert.Equal(t, "Metrics", route.ComponentTag.Component.ReportMeasures)
	assert.Equal(t, "Predicates", route.ComponentTag.Component.ReportFilters)
	assert.Equal(t, "Sort", route.ComponentTag.Component.ReportOrderBy)
	assert.Equal(t, "Take", route.ComponentTag.Component.ReportLimit)
	assert.Equal(t, "Skip", route.ComponentTag.Component.ReportOffset)
}

func TestStructScanner_Scan_QuerySelectorHolder(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &selectorHolderSource{}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)

	fields := descriptors.ByPath["ViewSelect.Fields"]
	require.NotNil(t, fields)
	assert.Equal(t, "rows", fields.QuerySelector)

	page := descriptors.ByPath["ViewSelect.Page"]
	require.NotNil(t, page)
	assert.Equal(t, "rows", page.QuerySelector)
}

func TestStructScanner_Scan_DynamicComponentHolderTypes(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET"`
	}{
		Route: xdatly.Component[any, any]{
			Inout:  dynamicReportInput{},
			Output: dynamicReportOutput{},
		},
	}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.Len(t, descriptors.ComponentFields, 1)
	route := descriptors.ComponentFields[0]
	require.NotNil(t, route)
	assert.Equal(t, reflect.TypeOf(dynamicReportInput{}), route.ComponentInputType)
	assert.Equal(t, reflect.TypeOf(dynamicReportOutput{}), route.ComponentOutputType)
}

func TestStructScanner_Scan_DynamicComponentHolderTypesWithExplicitNames(t *testing.T) {
	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
	}{}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.Len(t, descriptors.ComponentFields, 1)
	route := descriptors.ComponentFields[0]
	require.NotNil(t, route)
	assert.Nil(t, route.ComponentInputType)
	assert.Nil(t, route.ComponentOutputType)
	assert.Equal(t, "ReportInput", route.ComponentInputName)
	assert.Equal(t, "ReportOutput", route.ComponentOutputName)
}

func TestStructScanner_Scan_DynamicComponentHolderTypesWithExplicitNamesFromRegistry(t *testing.T) {
	scanner := New()
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(namedReportInput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/scan"), x.WithName("ReportInput")))
	registry.Register(x.NewType(reflect.TypeOf(namedReportOutput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/scan"), x.WithName("ReportOutput")))
	result, err := scanner.Scan(context.Background(), &shape.Source{
		Struct: &struct {
			Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
		}{},
		TypeRegistry: registry,
	})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	require.Len(t, descriptors.ComponentFields, 1)
	route := descriptors.ComponentFields[0]
	require.NotNil(t, route)
	assert.Equal(t, reflect.TypeOf(namedReportInput{}), route.ComponentInputType)
	assert.Equal(t, reflect.TypeOf(namedReportOutput{}), route.ComponentOutputType)
	assert.Equal(t, "ReportInput", route.ComponentInputName)
	assert.Equal(t, "ReportOutput", route.ComponentOutputName)
}

func TestStructScanner_Scan_DynamicComponentHolderTypesRequireContract(t *testing.T) {
	scanner := New()
	_, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET"`
	}{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic component holder requires explicit input/output tag names or initialized Inout/Output values")
}

func TestStructScanner_Scan_WithRegistryType(t *testing.T) {
	scanner := New()
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(reportSource{})))
	result, err := scanner.Scan(context.Background(), &shape.Source{
		TypeName:     "github.com/viant/datly/repository/shape/scan.reportSource",
		TypeRegistry: registry,
	})
	require.NoError(t, err)
	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf(reportSource{}), descriptors.RootType)
}

func TestStructScanner_Scan_UsesSourceBaseDirForRelativeSQL(t *testing.T) {
	scanner := New()
	baseDir := t.TempDir()
	sqlPath := filepath.Join(baseDir, "routes", "report.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sqlPath), 0o755))
	require.NoError(t, os.WriteFile(sqlPath, []byte("SELECT ID FROM REPORT"), 0o644))

	type reportView struct {
		Data []reportRow `view:"rows" sql:"uri=routes/report.sql"`
	}

	result, err := scanner.Scan(context.Background(), &shape.Source{
		Type: reflect.TypeOf(reportView{}),
		Path: filepath.Join(baseDir, "router.go"),
	})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)
	viewField := descriptors.ByPath["Data"]
	require.NotNil(t, viewField)
	require.NotNil(t, viewField.ViewTag)
	assert.Equal(t, "SELECT ID FROM REPORT", string(viewField.ViewTag.SQL.SQL))
	assert.Equal(t, "routes/report.sql", string(viewField.ViewTag.SQL.URI))
}

func TestStructScanner_Scan_RecursesIntoViewTaggedStructFields(t *testing.T) {
	type vendorProduct struct {
		ID       int `sqlx:"ID"`
		VendorID int `sqlx:"VENDOR_ID"`
	}
	type vendorRow struct {
		ID       int              `sqlx:"ID"`
		Products []*vendorProduct `view:",table=PRODUCT" on:"Id:ID=VendorId:VENDOR_ID" sql:"uri=testdata/report.sql"`
	}
	type nestedViewOutput struct {
		Data []*vendorRow `parameter:",kind=output,in=view" view:"vendor" sql:"uri=testdata/report.sql" anonymous:"true"`
	}
	type nestedViewRouteSource struct {
		Route xdatly.Component[reportInput, nestedViewOutput] `component:",path=/v1/api/dev/vendors,method=GET"`
	}

	scanner := New()
	result, err := scanner.Scan(context.Background(), &shape.Source{Struct: &nestedViewRouteSource{}})
	require.NoError(t, err)

	descriptors, ok := DescriptorsFrom(result)
	require.True(t, ok)

	rootView := descriptors.ByPath["Route.Output.Data"]
	require.NotNil(t, rootView)
	require.True(t, rootView.HasViewTag)

	childView := descriptors.ByPath["Route.Output.Data.Products"]
	require.NotNil(t, childView)
	require.True(t, childView.HasViewTag)
	require.NotNil(t, childView.ViewTag)
	assert.Equal(t, "PRODUCT", childView.ViewTag.View.Table)
	assert.Len(t, descriptors.ViewFields, 2)
	assert.Nil(t, descriptors.ByPath["Route.Output.Data.Products"].StateTag)
}
