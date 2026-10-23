package transcribe

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	x "github.com/viant/x"
	xast "github.com/viant/x/loader/ast"
	xload "github.com/viant/x/loader/xreflect"
	"github.com/viant/x/shape"
	"github.com/viant/x/syntetic/model"
)

//go:embed testdata/runtime_contract_artifact/reader.json
var embeddedRuntimeContract embed.FS

func TestRuntimeContractArtifactXNamedTimeAndAliasConflict(t *testing.T) {
	backtickTag := reflect.StructTag("view:\"table=`viant.reporting.records`\"")
	withQuotedTable := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeFor[string](), Tag: backtickTag}})
	quotedSource, err := renderArtifactXType(withQuotedTable)
	if err != nil {
		t.Fatal(err)
	}
	quotedRestored, err := materializeArtifactXType(quotedSource, nil)
	if err != nil || quotedRestored != withQuotedTable {
		t.Fatalf("quoted table tag changed: %v %v", quotedRestored, err)
	}
	typeOf := reflect.TypeFor[struct {
		When   time.Time
		Linked artifactLinkedValue
	}]()
	if RuntimeTypeFingerprint(nil) != "" || RuntimeTypeFingerprint(reflect.TypeFor[artifactLinkedValue]()) == "" {
		t.Fatal("linked type fingerprint is unavailable")
	}
	source, err := renderArtifactXType(typeOf)
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[artifactLinkedValue]())); err != nil {
		t.Fatal(err)
	}
	restored, err := materializeArtifactXType(source, catalog)
	if err != nil || restored != typeOf {
		t.Fatalf("named types changed: %v %v", restored, err)
	}
	if _, err = materializeArtifactXType(source, nil); err == nil {
		t.Fatal("missing linked type accepted")
	}
	conflict := "package reader\nimport (same \"example.com/one\"; same \"example.com/two\")\ntype Shape struct{ Value same.T }"
	if _, err = materializeArtifactXType(conflict, catalog); err == nil || !strings.Contains(err.Error(), "alias conflict") {
		t.Fatalf("conflicting imports accepted: %v", err)
	}
}

func TestRuntimeContractArtifactFSReadsEmbedAndDiskWithoutDiscovery(t *testing.T) {
	const artifactPath = "testdata/runtime_contract_artifact/reader.json"
	const artifactHash = "9ea267d201bcdcea9ff3891aac005a37f58764992e9c68b5991e2dafebe953d8"
	raw, err := fs.ReadFile(embeddedRuntimeContract, artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	disk := t.TempDir()
	if err = os.MkdirAll(filepath.Join(disk, "testdata/runtime_contract_artifact"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(disk, artifactPath), raw, 0600); err != nil {
		t.Fatal(err)
	}
	for name, folder := range map[string]fs.FS{"embed": embeddedRuntimeContract, "disk": os.DirFS(disk)} {
		t.Run(name, func(t *testing.T) {
			artifact, err := ReadRuntimeContractArtifactFS(folder, artifactPath, artifactHash)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ReadRuntimeContractArtifactFS(folder, artifactPath, strings.Repeat("0", 64)); err == nil {
				t.Fatal("wrong digest accepted")
			}
			contract, err := artifact.Materialize(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := report.NewProjectCompiler(report.ProjectConfig{Types: contract.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types}})
			if err != nil {
				t.Fatal(err)
			}
			connection, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err = connection.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, 'one'), (2, 'two')`); err != nil {
				t.Fatal(err)
			}
			reader, err := compiled.Artifacts()[0].ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: connection}})
			if err != nil {
				t.Fatal(err)
			}
			value, err := reader.Read(context.Background(), reflect.New(contract.InputType).Interface(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(value)
			if err != nil || !strings.Contains(string(encoded), "two") {
				t.Fatalf("hydrated output %s %v", encoded, err)
			}
		})
	}
}

// This tests a persisted compiler output, not a source or database discovery
// shortcut: the hydrated artifact enters the ordinary bootstrap compiler.
func TestRuntimeContractArtifactXSourceRoundtrip(t *testing.T) {
	contract := artifactFixture(t)
	frozen, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(frozen)
	if err != nil {
		t.Fatal(err)
	}
	source := renderArtifactShapes(t, contract.InputType, contract.OutputType)
	folder := fstest.MapFS{
		"go.mod":               &fstest.MapFile{Data: []byte("module example.com/artifact\n\ngo 1.25.8\n")},
		"compiled/types.go":    &fstest.MapFile{Data: []byte(source)},
		"compiled/reader.json": &fstest.MapFile{Data: raw},
	}
	hydrated := hydrateArtifactFS(t, folder, "compiled", contract.Resources)
	if hydrated.InputType != contract.InputType || hydrated.OutputType != contract.OutputType {
		t.Fatalf("x runtime shapes changed: input %v/%v output %v/%v", hydrated.InputType, contract.InputType, hydrated.OutputType, contract.OutputType)
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Types: hydrated.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: hydrated.Component, InputType: hydrated.InputType, OutputType: hydrated.OutputType, Types: hydrated.Types, Resources: hydrated.Resources}})
	if err != nil {
		t.Fatal(err)
	}
	if len(compilation.Artifacts()) == 0 || compilation.Artifacts()[0].ReaderCompilation() == nil {
		t.Fatal("hydrated reader did not reach bootstrap")
	}
}

func TestRuntimeContractArtifactEmbeddedSQLAndParameterRoundtrip(t *testing.T) {
	connection, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err = connection.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, 'one'), (2, 'two')`); err != nil {
		t.Fatal(err)
	}
	resources, err := resource.New().WithDefault(fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte("SELECT ID, NAME FROM records")}})
	if err != nil {
		t.Fatal(err)
	}
	discovery := &artifactCountingResolver{db: connection}
	contract, err := NewCompiler().RuntimeContractsInModule(context.Background(), "example.com/app", &Source{
		Scope: "example.com/app/readers/records", Name: "Records", Connector: "main", Resources: resources,
		ColumnRefiner: column.New(discovery),
		Text: `#package('example.com/app/readers/records')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#define($_ = $Floor<int>(query/floor).Optional())
#define($_ = $Rows<[]*Row>(output/view))
SELECT records.*, type(records,'Row') FROM (${embed:sql/records.sql}) records`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if discovery.calls.Load() == 0 {
		t.Fatal("live compiler did not discover connector")
	}
	if field, ok := contract.InputType.FieldByName("Floor"); !ok || field.Type.Kind() != reflect.Int || field.Tag.Get("parameter") == "" {
		t.Fatalf("parameter shape missing: %v", contract.InputType)
	}
	frozen, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(frozen)
	if err != nil {
		t.Fatal(err)
	}
	var restored RuntimeContractArtifact
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	beforeHit := discovery.calls.Load()
	if err = connection.Close(); err != nil {
		t.Fatal(err)
	}
	hydrated, err := restored.Materialize(nil, resources)
	if err != nil {
		t.Fatal(err)
	}
	if discovery.calls.Load() != beforeHit {
		t.Fatal("cache hit performed connector discovery")
	}
	if hydrated.InputType != contract.InputType || hydrated.OutputType != contract.OutputType {
		t.Fatal("parameter or row type changed")
	}
	compiled, err := report.NewProjectCompiler(report.ProjectConfig{Types: hydrated.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: hydrated.Component, InputType: hydrated.InputType, OutputType: hydrated.OutputType, Types: hydrated.Types, Resources: resources}})
	if err != nil {
		t.Fatal(err)
	}
	newConnection, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer newConnection.Close()
	if _, err = newConnection.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, 'one'), (2, 'two')`); err != nil {
		t.Fatal(err)
	}
	reader, err := compiled.Artifacts()[0].ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: newConnection}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Read(context.Background(), reflect.New(hydrated.InputType).Interface(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil || !strings.Contains(string(encoded), "two") {
		t.Fatalf("hydrated read: %s %v", encoded, err)
	}
}

type artifactCountingResolver struct {
	db    *sql.DB
	calls atomic.Int32
}

func (r *artifactCountingResolver) ResolveDB(_ context.Context, name string) (*sql.DB, error) {
	r.calls.Add(1)
	if name != "main" {
		return nil, fmt.Errorf("unexpected connector %s", name)
	}
	return r.db, nil
}

func BenchmarkRuntimeContractArtifactXMissVsHit(b *testing.B) {
	connection, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer connection.Close()
	if _, err = connection.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, 'one')`); err != nil {
		b.Fatal(err)
	}
	const sourceText = `#package('example.com/app/readers/records')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#define($_ = $Floor<int>(query/floor).Optional())
#define($_ = $Rows<[]*Row>(output/view))
SELECT records.*, type(records,'Row') FROM (SELECT * FROM records) records`
	compile := func() (*RuntimeContract, error) {
		return NewCompiler().RuntimeContractsInModule(context.Background(), "example.com/app", &Source{
			Scope: "example.com/app/readers/records", Name: "Records", Connector: "main", Text: sourceText,
			ColumnRefiner: column.New(column.Connections{"main": connection}),
		})
	}
	contract, err := compile()
	if err != nil {
		b.Fatal(err)
	}
	artifact, err := contract.Freeze()
	if err != nil {
		b.Fatal(err)
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		b.Fatal(err)
	}
	folder := fstest.MapFS{"reader.json": &fstest.MapFile{Data: raw}}
	key := sha256.Sum256(raw)
	keyString := hex.EncodeToString(key[:])
	b.Run("live-discovery-plus-bootstrap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fresh, e := compile()
			if e != nil {
				b.Fatal(e)
			}
			if _, e = report.NewProjectCompiler(report.ProjectConfig{Types: fresh.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: fresh.Component, InputType: fresh.InputType, OutputType: fresh.OutputType, Types: fresh.Types}}); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("fs-hit-x-hydrate-plus-bootstrap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stored, e := ReadRuntimeContractArtifactFS(folder, "reader.json", keyString)
			if e != nil {
				b.Fatal(e)
			}
			fresh, e := stored.Materialize(nil, nil)
			if e != nil {
				b.Fatal(e)
			}
			if _, e = report.NewProjectCompiler(report.ProjectConfig{Types: fresh.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: fresh.Component, InputType: fresh.InputType, OutputType: fresh.OutputType, Types: fresh.Types}}); e != nil {
				b.Fatal(e)
			}
		}
	})
}

func renderArtifactShapes(t *testing.T, input, output reflect.Type) string {
	t.Helper()
	file := &model.GoFile{PkgName: "compiled"}
	emptyInput := input.Kind() == reflect.Struct && input.NumField() == 0
	for _, item := range []struct {
		name string
		typ  reflect.Type
	}{{"Input", input}, {"Output", output}} {
		t.Logf("%s runtime type: %v", item.name, item.typ)
		if item.name == "Input" && emptyInput {
			continue
		}
		typ, err := xload.BuildType(item.typ, xload.WithPackagePath("example.com/artifact/compiled"), xload.WithNamePolicy(func(reflect.Type) (string, bool) { return item.name, false }))
		if err != nil {
			t.Fatal(err)
		}
		file.AddType(typ)
	}
	source, err := file.Render()
	if err != nil {
		t.Fatal(err)
	}
	if emptyInput {
		source += "\ntype Input struct{}\n"
	}
	return source
}

func hydrateArtifactFS(t *testing.T, folder fs.FS, dir string, resources *resource.Store) *RuntimeContract {
	t.Helper()
	raw, err := fs.ReadFile(folder, dir+"/reader.json")
	if err != nil {
		t.Fatal(err)
	}
	var frozen RuntimeContractArtifact
	if err = json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	pkg, err := xast.LoadPackageFS(context.Background(), folder, dir)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]reflect.Type{}
	for _, declared := range pkg.Types {
		if declared == nil {
			continue
		}
		aliases := map[string]string{}
		for _, imported := range declared.Imports {
			if imported != nil {
				aliases[imported.Alias] = imported.Path
			}
		}
		shapeType, shapeErr := (shape.Runtime{Imports: aliases, Lookup: func(name string) (reflect.Type, error) {
			if known := byName[name]; known != nil {
				return known, nil
			}
			return nil, nil
		}}).Type(declared.Body())
		if shapeErr != nil {
			t.Fatal(shapeErr)
		}
		byName[declared.Name] = shapeType
	}
	if byName["Input"] == nil || byName["Output"] == nil {
		t.Fatal("rendered shapes missing")
	}
	hydrated, err := frozen.Materialize(typecatalog.NewCatalog(), resources)
	if err != nil {
		t.Fatal(err)
	}
	if hydrated.InputType != byName["Input"] || hydrated.OutputType != byName["Output"] {
		t.Fatal("x shape path and artifact materializer disagree")
	}
	return hydrated
}
