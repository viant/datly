package transcribe

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

func TestNativeNestedGeneratedViewQualifiedDiscovery(t *testing.T) {
	const pkg = "example.com/records/generated"
	for _, declaration := range []string{"self.Dictionary", pkg + ".Dictionary"} {
		t.Run(declaration, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY, dictionary_id INTEGER)", "CREATE TABLE dictionaries(id INTEGER PRIMARY KEY, caption TEXT)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#setting($_ = $route('/records','GET'))
SELECT r.*, d.*,type(r,'self.Record'),type(d,'` + declaration + `','Dictionary')
FROM records r LEFT JOIN dictionaries d ON d.id=r.dictionary_id AND 1=1`
			compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: pkg, Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
			require.NoError(t, err)
			require.Len(t, compiled.Component.RootView.Relations, 1)
			child := compiled.Component.RootView.Relations[0].View
			require.Equal(t, declaration, child.TypeName)
			require.Len(t, child.Columns, 2, "native discovery must retain the SQL-derived row")
		})
	}
}

func TestNativeQualifiedRepeatedLeafRowsRemainCompatible(t *testing.T) {
	const pkg = "example.com/records/generated"
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY, first_id INTEGER, second_id INTEGER)", "CREATE TABLE dictionaries(id INTEGER PRIMARY KEY, caption TEXT)"))
	text := `#package('example.com/records/generated')
#import('self','example.com/records/generated')
#setting($_ = $route('/records','GET'))
SELECT r.*, a.*, b.*, type(r,'self.Record'),type(a,'self.Dictionary','First'),type(b,'self.Dictionary','Second')
FROM records r LEFT JOIN dictionaries a ON a.id=r.first_id AND 1=1
LEFT JOIN dictionaries b ON b.id=r.second_id AND 1=1`
	compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: pkg, Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	require.NoError(t, err)
	plan, err := gen.New(gen.Input{Component: compiled.Component, TargetPackage: pkg, TypeResolver: compiled.TypeResolver, ViewBindings: compiled.ViewBindings}).Plan()
	require.NoError(t, err)
	n := 0
	for _, view := range plan.Views {
		if view.Name == "Dictionary" {
			n++
		}
	}
	require.Equal(t, 1, n, "compatible repeated leaf declarations reuse one native row")
}

func TestNativeQualifiedReaderFieldUsesAuthoredPackage(t *testing.T) {
	const pkg = "example.com/records/generated"
	for _, qualifier := range []string{"self", pkg} {
		t.Run(qualifier, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY, caption TEXT, enabled INTEGER)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#setting($_ = $route('/records','GET'))
#define($_ = $ID<int>(query/id).WithPredicate(0,'equal','r','id').Optional())
#define($_ = $Data<[]*` + qualifier + `.Record>(output/view))
SELECT r.*,type(r,'Record') FROM records r`
			compile := func() *Result {
				compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/records/dql", Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
				require.NoError(t, err)
				return compiled
			}
			compiled := compile()
			require.Len(t, compiled.Component.RootView.Columns, 3)
			plan, err := gen.New(gen.Input{Component: compiled.Component, TargetPackage: pkg, TypeResolver: compiled.TypeResolver, ViewBindings: compiled.ViewBindings}).Plan()
			require.NoError(t, err)
			data, ok := plan.Output.Field("Data")
			require.True(t, ok)
			require.Equal(t, "[]*Record", data.Type)
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/records"}).Write(t, root)
			generate := func() *GeneratedPackage {
				generated, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Scope: "example.com/records/dql", Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}, Generation: GenerationOptions{Operation: "get"}})
				require.NoError(t, err)
				return generated
			}
			generated := generate()
			anchored := false
			for _, file := range generated.Result.Files {
				anchored = anchored || strings.Contains(file.Content, "var _anchorRecordsComponent = reflect.TypeFor[RecordsComponent]()")
				require.NotContains(t, file.Content, `self "`+pkg+`"`, "must not import its own package")
			}
			require.True(t, anchored)
			build := exec.Command("go", "test", "-mod=mod", "./generated")
			build.Dir = root
			output, err := build.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.NoError(t, db.ExecStatements(context.Background(), "ALTER TABLE records ADD COLUMN label TEXT"))
			regenerated := compile()
			require.Len(t, regenerated.Component.RootView.Columns, 4, "SQL schema remains row authority")
			updated := generate()
			row := updated.Result.Plan.ViewByType("Record")
			require.NotNil(t, row)
			_, ok = row.Field("Label")
			require.True(t, ok, "regeneration must emit the added column")
		})
	}
}

func TestNativeQualifiedReaderFieldRejectsUnownedOutput(t *testing.T) {
	const pkg = "example.com/records/generated"
	for _, tc := range []struct{ name, reference, declaration string }{
		{"no explicit SQL declaration", "self.Record", ""},
		{"mismatched declaration", "self.Record", ",type(r,'Other')"},
		{"foreign same name", "foreign.Record", ",type(r,'Record')"},
		{"unknown alias", "missing.Record", ",type(r,'Record')"},
		{"source namespace", "example.com/records/dql.Record", ",type(r,'Record')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY, caption TEXT)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#import('foreign','example.com/records/other')
#setting($_ = $route('/records','GET'))
#define($_ = $Data<[]*` + tc.reference + `>(output/view))
SELECT r.*` + tc.declaration + ` FROM records r`
			_, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/records/dql", Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
			require.Error(t, err)
		})
	}
}
