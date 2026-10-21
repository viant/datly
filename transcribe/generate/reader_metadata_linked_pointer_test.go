package generate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type metadataLinkedPointerRow struct {
	ID *int `sqlx:"physical_id|ID"`
}
type metadataLinkedPointerOutput struct {
	Data []*metadataLinkedPointerRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func linkedPointerMetadataInput(t *testing.T) Input {
	t.Helper()
	return linkedPointerMetadataInputFor(t, reflect.TypeFor[metadataLinkedPointerRow](), reflect.TypeFor[metadataLinkedPointerOutput]())
}

func linkedPointerMetadataInputFor(t *testing.T, rowType, outputType reflect.Type) Input {
	t.Helper()
	input := reviewLinkedEligibilityInput(t, false)
	row := x.NewType(rowType)
	output := x.NewType(outputType)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, row, output, x.NewType(reflect.TypeFor[reviewLinkedInput]())))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	input.TypeResolver = resolver
	input.Contracts.Output = &ContractReference{Expression: output.Name, DescriptorKey: output.Key()}
	input.Views = ViewReferences{RootViewPath: &ViewReference{DescriptorKey: row.Key()}}
	input.Component.Parameters[0].TypeExpr = "[]*" + row.Key()
	input.Component.RootView.TypeName = row.Name
	input.Component.RootView.Columns[0].NotNull = true
	input.Component.RootView.Columns[0].Source = "physical_id"
	input.Component.RootView.Source.SQL = "SELECT physical_id AS ID FROM records"
	require.NoError(t, os.WriteFile(filepath.Join(input.ProjectRoot, "generate", "sql", "records.sql"), []byte(input.Component.RootView.Source.SQL), 0600))
	return input
}

func metadataDocument(t *testing.T, plan *Plan) (*readergraph.Document, string) {
	t.Helper()
	for _, file := range plan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			document, err := readergraph.StrictDecode([]byte(file.Content))
			require.NoError(t, err)
			return document, file.Content
		}
	}
	t.Fatal("missing native metadata")
	return nil, ""
}

func TestReaderMetadataLinkedInferredScalarPointer(t *testing.T) {
	input := linkedPointerMetadataInput(t)
	before, err := json.Marshal(input.Component)
	require.NoError(t, err)
	plan, err := New(input).Plan()
	require.NoError(t, err)
	document, body := metadataDocument(t, plan)
	column := document.Component.RootView.Columns[0]
	require.Equal(t, spec.TypeRef{Name: "int", Pointer: true}, column.Type)
	require.False(t, column.Nullable)
	require.True(t, column.NotNull)
	require.False(t, column.ExplicitType)
	require.Equal(t, "physical_id", column.Source)
	require.NoError(t, readergraph.CheckRows(document, input.TypeResolver.Descriptor))
	again, err := New(input).Plan()
	require.NoError(t, err)
	_, second := metadataDocument(t, again)
	require.Equal(t, body, second)
	after, err := json.Marshal(input.Component)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
	require.Equal(t, reflect.TypeFor[*int](), reflect.TypeFor[metadataLinkedPointerRow]().Field(0).Type)

	// Execute the strict-decoded destination through the ordinary SQL reader.
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(physical_id INTEGER NOT NULL)", "INSERT INTO records VALUES(7),(0)"))
	graph, err := readergraph.AdmitWithLookup(document.Component, x.NewType(reflect.TypeFor[reviewLinkedInput]()), x.NewType(reflect.TypeFor[metadataLinkedPointerOutput]()), map[string][]byte{document.Component.RootView.Source.URI: []byte(input.Component.RootView.Source.SQL)}, input.TypeResolver.Descriptor)
	require.NoError(t, err)
	runtimePlan, err := compiler.Compile(compiler.Input{Component: graph, Resources: input.Resources, InputType: reflect.TypeFor[reviewLinkedInput](), OutputType: reflect.TypeFor[metadataLinkedPointerOutput](), DirectViewField: "Data"})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: graph, InputType: reflect.TypeFor[reviewLinkedInput](), OutputType: reflect.TypeFor[metadataLinkedPointerOutput](), Plan: runtimePlan, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	result, err := execution.Read(ctx, &reviewLinkedInput{}, nil, nil)
	require.NoError(t, err)
	rows := result.(*metadataLinkedPointerOutput).Data
	require.Len(t, rows, 2)
	require.NotNil(t, rows[0].ID)
	require.NotNil(t, rows[1].ID)
	require.Equal(t, 7, *rows[0].ID)
	require.Zero(t, *rows[1].ID)
}

func TestReaderMetadataLinkedPointerRejectsIncompatibleAuthority(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      spec.TypeRef
		explicit bool
		wanted   string
	}{
		{"explicit cast", spec.TypeRef{Name: "int"}, true, "CAST cannot change"},
		{"different base", spec.TypeRef{Name: "string"}, false, "differs from *int"},
		{"pointer depth", spec.TypeRef{Name: "*int", Pointer: true}, false, "differs from *int"},
		{"slice", spec.TypeRef{Name: "int", Cardinality: spec.CardinalityMany}, false, "differs from *int"},
		{"pointer to slice", spec.TypeRef{Name: "int", Cardinality: spec.CardinalityMany, SlicePointer: true}, false, "differs from *int"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := linkedPointerMetadataInput(t)
			input.Component.RootView.Columns[0].Type = tc.typ
			input.Component.RootView.Columns[0].ExplicitType = tc.explicit
			_, err := New(input).Plan()
			require.ErrorContains(t, err, tc.wanted)
		})
	}
}

type metadataLinkedSlice []int
type metadataLinkedCollectionRow struct {
	Value *metadataLinkedSlice `sqlx:"physical_id|ID"`
}
type metadataLinkedCollectionOutput struct {
	Data []*metadataLinkedCollectionRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func TestReaderMetadataLinkedNamedCollectionUnaffected(t *testing.T) {
	input := linkedPointerMetadataInputFor(t, reflect.TypeFor[metadataLinkedCollectionRow](), reflect.TypeFor[metadataLinkedCollectionOutput]())
	input.Component.RootView.Columns[0].Type = spec.TypeRef{Name: "metadataLinkedSlice", Package: reflect.TypeFor[metadataLinkedSlice]().PkgPath()}
	_, err := New(input).Plan()
	require.ErrorContains(t, err, "differs from *")
}

type metadataLinkedAliasRow struct {
	Value *int `sqlx:"physical_id|ID"`
}
type metadataLinkedAliasOutput struct {
	Data []*metadataLinkedAliasRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func TestReaderMetadataLinkedPointerNativeAlias(t *testing.T) {
	input := linkedPointerMetadataInputFor(t, reflect.TypeFor[metadataLinkedAliasRow](), reflect.TypeFor[metadataLinkedAliasOutput]())
	plan, err := New(input).Plan()
	require.NoError(t, err)
	document, _ := metadataDocument(t, plan)
	require.True(t, document.Component.RootView.Columns[0].Type.Pointer)
	require.NoError(t, readergraph.CheckRows(document, input.TypeResolver.Descriptor))
}
