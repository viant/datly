package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type qualifiedSummaryCatalogRow struct {
	Count int `sqlx:"count"`
}

func TestNativeQualifiedSQLSummaryGeneratedOwnership(t *testing.T) {
	const pkg = "example.com/summary/generated"
	for _, qualifier := range []string{"self", pkg, "*self", "*" + pkg} {
		t.Run(qualifier, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY, label TEXT)", "CREATE TABLE totals(count INTEGER NOT NULL)", "INSERT INTO totals VALUES(2)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#setting($_ = $route('/records','GET'))
#define($_ = $Meta<` + qualifier + `.SummaryView>(output/summary) /* SELECT total.*, type(total,'` + strings.TrimPrefix(qualifier, "*") + `.SummaryView') FROM totals total */)
#define($_ = $Data<[]*` + qualifier + `.Record>(output/view))
SELECT r.*,type(r,'Record') FROM records r`
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/summary"}).Write(t, root)
			catalog := typecatalog.NewCatalog()
			generate := func() *GeneratedPackage {
				generated, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Scope: "example.com/summary/dql", Name: "Records", Connector: "main", Text: text, Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB})}, Generation: GenerationOptions{Operation: "get"}})
				require.NoError(t, err)
				return generated
			}
			first := generate()
			require.Equal(t, gen.ViewGenerated, first.Result.Plan.ViewByType("SummaryView").Ownership)
			meta, ok := first.Result.Plan.Output.Field("Meta")
			require.True(t, ok)
			expectedType := "SummaryView"
			if strings.HasPrefix(qualifier, "*") {
				expectedType = "*SummaryView"
			}
			require.Equal(t, expectedType, meta.Type)
			canonicalType := pkg + ".SummaryView"
			if strings.HasPrefix(qualifier, "*") {
				canonicalType = "*" + canonicalType
			}
			parameterTag := reflect.StructTag(meta.Tag).Get("parameter")
			dataType := ""
			for _, part := range strings.Split(parameterTag, ",") {
				if strings.HasPrefix(part, "dataType=") {
					dataType = strings.TrimPrefix(part, "dataType=")
				}
			}
			actualIdentity, err := first.Result.Plan.CanonicalType(pkg, dataType)
			require.NoError(t, err)
			require.Equal(t, canonicalType, actualIdentity, "emitted metadata must resolve under exact package authority")
			require.NoError(t, catalog.Register(typecatalog.TypeOriginGenerated, x.NewType(reflect.TypeFor[qualifiedSummaryCatalogRow](), x.WithPkgPath(pkg), x.WithName("SummaryView"))))
			stable := generate()
			require.Equal(t, first.Result.Files, stable.Result.Files, "regeneration must retain SQL ownership with existing generated descriptors")
			require.NoError(t, db.ExecStatements(context.Background(), "ALTER TABLE totals ADD COLUMN sample_count INTEGER"))
			updated := generate()
			summary := updated.Result.Plan.ViewByType("SummaryView")
			require.NotNil(t, summary)
			_, ok = summary.Field("SampleCount")
			require.True(t, ok, "SQL schema must still own qualified summary shape")
			for _, file := range updated.Result.Files {
				require.NotContains(t, file.Content, `self "`+pkg+`"`)
			}
			build := exec.Command("go", "test", "-mod=mod", "./generated")
			build.Dir = root
			out, err := build.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}

func TestNativeQualifiedSQLSummaryRejectsUnownedType(t *testing.T) {
	const pkg = "example.com/summary/generated"
	for _, tc := range []struct{ name, reference, declaration string }{
		{"missing declaration", "self.SummaryView", ""},
		{"mismatched declaration", "self.SummaryView", ",type(total,'Different')"},
		{"foreign same name", "foreign.SummaryView", ",type(total,'SummaryView')"},
		{"unknown alias", "missing.SummaryView", ",type(total,'SummaryView')"},
		{"source namespace", "example.com/summary/dql.SummaryView", ",type(total,'SummaryView')"},
		{"collection holder", "[]*self.SummaryView", ",type(total,'SummaryView')"},
		{"double pointer", "**self.SummaryView", ",type(total,'SummaryView')"},
		{"spaced double pointer", "* *self.SummaryView", ",type(total,'SummaryView')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE totals(count INTEGER NOT NULL)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#import('foreign','example.com/summary/other')
#setting($_ = $route('/records','GET'))
#define($_ = $Meta<` + tc.reference + `>(output/summary) /* SELECT total.*` + tc.declaration + ` FROM totals total */)
SELECT r.*,type(r,'Record') FROM records r`
			_, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/summary/dql", Name: "Records", Connector: "main", Text: strings.TrimSpace(text), Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
			require.Error(t, err)
		})
	}
}

func TestNativeQualifiedSummaryExecution(t *testing.T) {
	const pkg = "github.com/viant/datly/summaryfixture/generated"
	for _, holder := range []string{"self.MetaView", "*self.MetaView"} {
		t.Run(holder, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE orders(id INTEGER, tenant_id INTEGER, name TEXT)"))
			text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#setting($_ = $route('/orders','GET'))
#set($_ = $TenantId<int>(query/tenant).Value(7))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(orderRows).Optional())
#set($_ = $Limit<int>(query/limit).QuerySelector(orderRows).Optional().Value(2))
#set($_ = $Offset<int>(query/offset).QuerySelector(orderRows).Optional())
#set($_ = $Meta<` + holder + `>(output/summary) /*
SELECT stats.*,type(stats,'self.MetaView'),CAST(stats.PAGE_COUNT AS int),CAST(stats.RECORD_COUNT AS int)
FROM (
SELECT CAST((COUNT(1) + COALESCE(NULLIF($View.Limit, 0), 1) - 1) / COALESCE(NULLIF($View.Limit, 0), 1) AS INTEGER) AS PAGE_COUNT,
COUNT(1) AS RECORD_COUNT FROM ($View.NonWindowSQL) t
) stats
*/)
#set($_ = $Data<?>(output/view))
SELECT orderRows.id, orderRows.name FROM orders orderRows WHERE orderRows.tenant_id = $TenantId ORDER BY orderRows.id`
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/summaryfixture"}).Write(t, root)
			generated, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Name: "Orders", Scope: "github.com/viant/datly/summaryfixture/orders", Connector: "main", Types: typecatalog.NewCatalog(), Text: text, ColumnRefiner: column.New(column.Connections{"main": db.DB})}, Generation: GenerationOptions{Operation: "get"}})
			require.NoError(t, err)
			require.Equal(t, gen.ViewGenerated, generated.Result.Plan.ViewByType("MetaView").Ownership)
			fixture := strings.Replace(generatedSummaryTest, "package orders", "package generated", 1)
			fixture = strings.ReplaceAll(fixture, "DatlyResourceNamespace", generated.Result.Plan.Resources.Symbol+"DatlyResourceNamespace")
			fixture = strings.ReplaceAll(fixture, "DatlyResources", generated.Result.Plan.Resources.Symbol+"DatlyResources")
			require.NoError(t, os.WriteFile(filepath.Join(root, "generated", "summary_test.go"), []byte(fixture), 0600))
			command := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./generated")
			command.Dir = root
			out, err := command.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}
