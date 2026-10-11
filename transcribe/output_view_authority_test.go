package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/outputrows"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const declaredOutputRowDQL = `#package('generated')
#import('rows','github.com/viant/datly/transcribe/testdata/outputrows')
#setting($_ = $route('/records', 'GET'))
#set($_ = $Scope<string>(query/scope))
#set($_ = $Data<[]*rows.Row>(output/view))
SELECT record.*, metrics.*, use_connector(metrics, 'offline')
FROM (SELECT id FROM records WHERE owner=$Scope) record
JOIN (SELECT id, stamp, spend FROM metrics) metrics ON record.id=metrics.id
`

func TestTranscribeGeneratedParentLinksDeclaredRelatedRow(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputrows.Metric]())))
	source := `#package('generated')
#import('rows','github.com/viant/datly/transcribe/testdata/outputrows')
#setting($_ = $route('/records', 'GET'))
#set($_ = $Data<?>(output/view))
SELECT record.*, cast(record.id AS int), metrics.*, type(metrics, 'rows.Metric'), use_connector(metrics, 'offline')
FROM (SELECT 1 AS id) record
JOIN (SELECT id, stamp, spend FROM metrics) metrics ON record.id=metrics.id
`
	generated, err := NewCompiler().Transcribe(context.Background(), Request{
		Destination: root,
		Source:      &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: source},
		Generation:  GenerationOptions{Operation: "get"},
	})
	require.NoError(t, err)
	var linked int
	for _, view := range generated.Result.Plan.Views {
		if view.Ownership == gen.ViewLinked {
			linked++
			require.Equal(t, "Metric", view.Name)
			require.Equal(t, "rows.Metric", view.Type)
		}
	}
	require.Equal(t, 1, linked)
	for _, rejection := range []struct{ name, projection, failure string }{
		{"incompatible cast", "cast(metrics.spend AS int)", "uneditable linked field"},
		{"relocated child", "dest(metrics, 'other/metrics.go')", "destination"},
	} {
		t.Run(rejection.name, func(t *testing.T) {
			destination := t.TempDir()
			testharness.WriteGeneratedGoMod(t, destination)
			invalid := strings.Replace(source, "metrics.*,", "metrics.*, "+rejection.projection+",", 1)
			_, err := NewCompiler().Transcribe(context.Background(), Request{Destination: destination, Source: &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: invalid}, Generation: GenerationOptions{Operation: "get"}})
			require.ErrorContains(t, err, rejection.failure)
		})
	}
	var holder bool
	for _, file := range generated.Result.Files {
		require.NotContains(t, file.Content, "type Metric struct")
		if strings.Contains(file.Content, "[]*rows.Metric") {
			holder = true
		}
	}
	require.True(t, holder, "generated parent must reference the existing related row")
	fixture := `package fixture_test
import (
 "context"; "database/sql"; "encoding/json"; "net/http/httptest"; "path/filepath"; "testing"
 _ "modernc.org/sqlite"
 _ "GENERATED_PACKAGE"
 "github.com/viant/datly/bootstrap/connector"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
)
func TestRelatedRow(t *testing.T) {
 dsn:=filepath.Join(t.TempDir(),"metrics.db"); db,err:=sql.Open("sqlite",dsn); if err!=nil {t.Fatal(err)}
 _,err=db.Exec("CREATE TABLE metrics(id INTEGER,stamp TEXT,spend REAL); INSERT INTO metrics VALUES(1,NULL,2.5),(2,NULL,9)"); if err!=nil {t.Fatal(err)}; db.Close()
 root,err:=filepath.Abs("."); if err!=nil {t.Fatal(err)}
 ctx:=context.Background(); server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:root,GoBootstrap:&config.Packages{Packages:[]string{"GENERATED_PACKAGE"}},Connectors:[]connector.Config{{Name:"offline",Driver:"sqlite",DSN:dsn}},Connector:"offline"}}); if err!=nil {t.Fatal(err)}; defer server.Shutdown(ctx)
 if err=server.Reload(ctx,1);err!=nil {t.Fatal(err)}
 rec:=httptest.NewRecorder();server.ServeHTTP(rec,httptest.NewRequest("GET","/records",nil));if rec.Code!=200 {t.Fatalf("%d %s",rec.Code,rec.Body.String())}
 var result struct {Data []struct {Id int; Metrics []struct {ID int64; Stamp *string; Spend *float64}}}
 if err=json.Unmarshal(rec.Body.Bytes(),&result);err!=nil {t.Fatal(err)}
 if len(result.Data)!=1||result.Data[0].Id!=1||len(result.Data[0].Metrics)!=1||result.Data[0].Metrics[0].ID!=1||result.Data[0].Metrics[0].Stamp!=nil||result.Data[0].Metrics[0].Spend==nil||*result.Data[0].Metrics[0].Spend!=2.5 {t.Fatalf("relation contract: %s",rec.Body.String())}
}
`
	fixture = strings.ReplaceAll(fixture, "GENERATED_PACKAGE", generated.Result.Plan.Package)
	require.NoError(t, os.WriteFile(filepath.Join(root, "runtime_test.go"), []byte(fixture), 0600))
	command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout=120s", ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestTranscribeGetLinksDeclaredOutputRowWithoutDatabase(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputrows.Row]())))
	generated, err := NewCompiler().Transcribe(context.Background(), Request{
		Destination: root,
		Source:      &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: declaredOutputRowDQL},
		Generation:  GenerationOptions{Operation: "get"},
	})
	require.NoError(t, err)
	plan := generated.Result.Plan
	require.Len(t, plan.Views, 1)
	require.Equal(t, gen.ViewLinked, plan.Views[0].Ownership)
	require.Equal(t, "github.com/viant/datly/transcribe/testdata/outputrows", plan.Views[0].Package)
	require.Equal(t, "rows.Row", plan.RootViewType)
	for _, file := range generated.Result.Files {
		require.NotContains(t, file.Content, "type Row struct")
		require.NotContains(t, file.Content, "type MetricsView struct")
		if strings.HasSuffix(file.Path, "output.go") {
			require.Contains(t, file.Content, "[]*rows.Row")
		}
	}
}

func TestDeclaredOutputRowRequiresAuthority(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: typecatalog.NewCatalog(), Text: declaredOutputRowDQL})
	require.ErrorContains(t, err, "requires existing named Go type authority")
}

func TestDeclaredOutputRowPreservesPriorReference(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputrows.Row]())))
	compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: declaredOutputRowDQL})
	require.NoError(t, err)
	prior := compiled.Views[gen.RootViewPath]
	require.NotNil(t, prior)
	borrowed := &gen.BorrowedRowAuthority{OwnerSource: "owner.dql", BorrowerSource: "borrower.dql"}
	prior.Borrowed = borrowed
	require.NoError(t, linkDeclaredOutputView(compiled, compiled.Component, nil))
	require.Same(t, prior, compiled.Views[gen.RootViewPath])
	require.Same(t, borrowed, compiled.Views[gen.RootViewPath].Borrowed)
	prior.DescriptorKey = "example.com/conflicting.Row"
	require.ErrorContains(t, linkDeclaredOutputView(compiled, compiled.Component, nil), "conflicting row type authorities")
	require.Same(t, prior, compiled.Views[gen.RootViewPath], "rejected authority must remain intact")
}

func TestTranscribeGetPreservesLocalLinkedRowsAndSQLResources(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	packagePath := "example.com/generated/records"
	dir := filepath.Join(root, "records")
	fixtureSource, err := os.ReadFile("testdata/outputrows/rows.go")
	require.NoError(t, err)
	rowSource := "// Code generated by Datly for Records; DO NOT EDIT.\n" + strings.Replace(string(fixtureSource), "package outputrows", "package records", 1)
	initial := &gen.Plan{
		Package: packagePath, GoPackage: "records", ComponentName: "Records", RouterDest: "router.go",
		Input:     gen.ContractPlan{Type: "RecordsInput", Destination: "input.go", Ownership: gen.ContractGenerated},
		Output:    gen.ContractPlan{Type: "RecordsOutput", Destination: "output.go", Ownership: gen.ContractGenerated},
		Resources: &gen.ResourcePlan{Namespace: "original_records", Destination: "resources.go", Symbol: "Records", Files: []gen.EmittedFile{{Path: "sql/metrics.sql", Content: "SELECT id, stamp, spend FROM metrics"}}},
	}
	_, err = gen.EmitScaffold(dir, initial)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "views.go"), []byte(rowSource), 0600))
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputrows.Row](), x.WithPkgPath(packagePath))))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputrows.Total](), x.WithPkgPath(packagePath))))
	source := strings.ReplaceAll(declaredOutputRowDQL, "#package('generated')", "#package('records')")
	source = strings.ReplaceAll(source, "github.com/viant/datly/transcribe/testdata/outputrows", packagePath)
	source = strings.Replace(source, "SELECT record.*", "#set($_ = $Meta<*rows.Total>(output/summary) /* SELECT COUNT(1) AS count FROM records */)\nSELECT record.*", 1)
	for i := 0; i < 2; i++ {
		generated, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root,
			Source:     &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: source},
			Generation: GenerationOptions{Operation: "get"},
		})
		require.NoError(t, err)
		require.Equal(t, "original_records", generated.Result.Plan.Resources.Namespace)
		require.Len(t, generated.Result.Plan.Views, 2)
		for _, view := range generated.Result.Plan.Views {
			require.Equal(t, gen.ViewLinked, view.Ownership)
		}
		actual, err := os.ReadFile(filepath.Join(dir, "views.go"))
		require.NoError(t, err)
		require.Equal(t, rowSource, string(actual))
		actual, err = os.ReadFile(filepath.Join(dir, "sql/metrics.sql"))
		require.NoError(t, err)
		require.Equal(t, "SELECT id, stamp, spend FROM metrics", string(actual))
		actual, err = os.ReadFile(filepath.Join(dir, "output.go"))
		require.NoError(t, err)
		require.Contains(t, string(actual), "[]*Row")
		require.NotContains(t, string(actual), "dataType=[]*rows.Row")
		require.Contains(t, string(actual), "dataType=[]*example.com/generated/records.Row")
		require.Contains(t, string(actual), "dataType=*example.com/generated/records.Total")
		require.Contains(t, string(actual), "original_records:sql/records.sql")
	}
	// A generated derived row cannot replace a file owning a linked root.
	_, err = NewCompiler().Transcribe(context.Background(), Request{Destination: root,
		Source:     &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: strings.Replace(source, "Meta<*rows.Total>", "Meta<?>", 1)},
		Generation: GenerationOptions{Operation: "get"},
	})
	require.ErrorContains(t, err, "contains a linked Go type")
	actual, err := os.ReadFile(filepath.Join(dir, "views.go"))
	require.NoError(t, err)
	require.Equal(t, rowSource, string(actual))
}
