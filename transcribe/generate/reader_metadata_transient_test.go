package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type metadataTransientRow struct {
	ID       int     `sqlx:"ID" json:"-" internal:"true"`
	Computed *string `sqlx:"-" json:"computed"`
}
type metadataTransientOutput struct {
	Data []*metadataTransientRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func TestReaderMetadataOmitsOnlySQLTransientScanColumns(t *testing.T) {
	input := reviewLinkedEligibilityInput(t, false)
	row := x.NewType(reflect.TypeFor[metadataTransientRow]())
	output := x.NewType(reflect.TypeFor[metadataTransientOutput]())
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, row, output, x.NewType(reflect.TypeFor[reviewLinkedInput]())))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	input.TypeResolver = resolver
	input.Contracts.Output = &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}
	input.Views = ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}
	input.Component.Parameters[0].TypeExpr = "[]*" + row.Key()
	input.Component.RootView.TypeName = row.Name
	const sql = "SELECT ID, '' AS COMPUTED FROM records"
	input.Component.RootView.Source.SQL = sql
	input.Component.RootView.Columns = []*spec.Column{
		{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"ID" json:"-" internal:"true"`},
		{Name: "COMPUTED", Source: "COMPUTED", Type: spec.TypeRef{Name: "string", Pointer: true}, Tag: `sqlx:"-" json:"computed"`},
	}
	sqlPath := filepath.Join(input.ProjectRoot, "generate", "sql", "records.sql")
	require.NoError(t, os.WriteFile(sqlPath, []byte(sql), 0600))
	before, err := json.Marshal(input.Component)
	require.NoError(t, err)
	plan, err := New(input).Plan()
	require.NoError(t, err)
	require.NotEmpty(t, plan.ReaderMetadata)
	var metadata *readergraph.Document
	sqlFound := false
	for _, file := range plan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			metadata, err = readergraph.StrictDecode([]byte(file.Content))
			require.NoError(t, err)
		}
		if file.Path == "sql/records.sql" {
			sqlFound = true
			require.Equal(t, sql, file.Content)
		}
	}
	require.True(t, sqlFound)
	require.NotNil(t, metadata)
	require.Len(t, metadata.Component.RootView.Columns, 1)
	require.Equal(t, "ID", metadata.Component.RootView.Columns[0].Name, "JSON/internal visibility does not suppress SQL mapping")
	require.NoError(t, readergraph.CheckRows(metadata, resolver.Descriptor), "serialized metadata reload must retain exact row authority")
	after, err := json.Marshal(input.Component)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "live canonical graph must keep logical hook-owned columns")
	bytes, err := os.ReadFile(sqlPath)
	require.NoError(t, err)
	require.Equal(t, sql, string(bytes), "native SQL placeholder must remain unchanged")
	field, ok := reflect.TypeFor[metadataTransientRow]().FieldByName("Computed")
	require.True(t, ok)
	require.Equal(t, "-", field.Tag.Get("sqlx"))
}
