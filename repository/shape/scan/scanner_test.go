package scan

import (
	"context"
	"embed"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/x"
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
	Rows []reportRow `view:"rows,table=REPORT,connector=dev" sql:"uri=testdata/report.sql"`
	ID   int         `parameter:"id,kind=query,in=id"`
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
	assert.Contains(t, rows.ViewTag.SQL.SQL, "SELECT ID, NAME FROM REPORT")

	idField := descriptors.ByPath["ID"]
	require.NotNil(t, idField)
	require.True(t, idField.HasStateTag)
	require.NotNil(t, idField.StateTag)
	require.NotNil(t, idField.StateTag.Parameter)
	assert.Equal(t, "id", idField.StateTag.Parameter.Name)
	assert.Equal(t, "query", idField.StateTag.Parameter.Kind)
	assert.Equal(t, "id", idField.StateTag.Parameter.In)
}

func TestStructScanner_Scan_InvalidSource(t *testing.T) {
	scanner := New()
	_, err := scanner.Scan(context.Background(), &shape.Source{Struct: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct")
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
