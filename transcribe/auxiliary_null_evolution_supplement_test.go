package transcribe

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestAuxiliaryNullWildcardEvolutionAndStableGeneration(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)", "CREATE TABLE evidence(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES records(id))"); err != nil {
		t.Fatal(err)
	}
	resources := resource.New()
	if err := resources.Register("", fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte("SELECT * FROM (records)")}, "sql/evidence.sql": &fstest.MapFile{Data: []byte("SELECT * FROM (evidence)")}}); err != nil {
		t.Fatal(err)
	}
	text := `#setting($_ = $route('/records','PATCH'))
#define($_ = $Rows<[]*RecordsView>(body/data).Cardinality('Many').Required())
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.*,e.*,root_null_policy(r,'skip-auxiliary'),nested_null_policy(e,'skip-auxiliary')
FROM (${embed:sql/records.sql}) r JOIN (${embed:sql/evidence.sql}) e ON e.parent_id=r.id`
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/auxiliaryevolution"}).Write(t, root)
	generate := func() map[string]string {
		t.Helper()
		source := &Source{Scope: "example.com/auxiliaryevolution/generated", Name: "Records", Connector: "main", Text: text, Resources: resources, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
		_, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}})
		if err != nil {
			t.Fatal(err)
		}
		if source.Text != text {
			t.Fatal("DQL changed during generation")
		}
		got := map[string]string{}
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			got[rel] = string(b)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			t.Fatal("no generated Go files")
		}
		return got
	}
	before := generate()
	if repeated := generate(); !reflect.DeepEqual(before, repeated) {
		t.Fatal("initial generation is not byte stable")
	}
	if err := db.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN added_note TEXT", "ALTER TABLE evidence ADD COLUMN evidence_label TEXT"); err != nil {
		t.Fatal(err)
	}
	after := generate()
	if reflect.DeepEqual(before, after) {
		t.Fatal("schema evolution did not change generated shapes")
	}
	all := ""
	for _, body := range after {
		all += body
	}
	for _, name := range []string{"added_note", "evidence_label"} {
		if !strings.Contains(all, name) {
			t.Fatalf("new column %s is absent from generated Go", name)
		}
	}
	if repeated := generate(); !reflect.DeepEqual(after, repeated) {
		t.Fatal("evolved generation is not byte stable")
	}
	for name, want := range map[string]string{"sql/records.sql": "SELECT * FROM (records)", "sql/evidence.sql": "SELECT * FROM (evidence)"} {
		got, err := resources.ReadFile(name)
		if err != nil || string(got) != want {
			t.Fatalf("source resource %s changed: %v", name, err)
		}
	}
}

func TestAuxiliaryNullNamedURIResourceAuthority(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)", "CREATE TABLE evidence(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES records(id))"); err != nil {
		t.Fatal(err)
	}
	resources := resource.New()
	if err := resources.Register("private", fstest.MapFS{"records.sql": &fstest.MapFile{Data: []byte("SELECT * FROM (records)")}, "evidence.sql": &fstest.MapFile{Data: []byte("SELECT * FROM (evidence)")}}); err != nil {
		t.Fatal(err)
	}
	text := `#package('example.com/auxiliaryuri/generated')

#setting($_ = $route('/records','PATCH'))
#define($_ = $Rows<[]*RecordsView>(body/data).Cardinality('Many').Required())
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.*,e.*,root_null_policy(r,'skip-auxiliary'),nested_null_policy(e,'skip-auxiliary')
FROM (SELECT * FROM (records)) r JOIN (SELECT * FROM (evidence)) e ON e.parent_id=r.id`
	compiled, err := NewCompiler().Compile(ctx, &Source{Scope: "example.com/auxiliaryuri/generated", Name: "Records", Connector: "main", Text: text, Resources: resources, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	if err != nil {
		t.Fatal(err)
	}
	rootView := compiled.Component.RootView
	child := rootView.Relations[0].View
	rootView.Source.SQL = ""
	rootView.Source.URI = "private:records.sql"
	child.Source.SQL = ""
	child.Source.URI = "private:evidence.sql"
	destination := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/auxiliaryuri"}).Write(t, destination)
	request := GenerationRequest{Compiled: compiled, Destination: destination}
	first, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Result.Files, second.Result.Files) {
		t.Fatal("named URI generation changed on repeat")
	}
	if rootView.Source.SQL != "" || rootView.Source.URI != "private:records.sql" || child.Source.SQL != "" || child.Source.URI != "private:evidence.sql" {
		t.Fatal("canonical URI source mutated")
	}
	found := map[string]bool{}
	for _, file := range first.Result.Files {
		for _, table := range []string{"records", "evidence"} {
			if strings.HasSuffix(file.Path, ".sql") && strings.Contains(file.Content, table) {
				found[table] = true
			}
		}
	}
	if !found["records"] || !found["evidence"] {
		t.Fatalf("URI assets absent from generation: %v", found)
	}
	rootView.Source.URI = "private:missing.sql"
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err == nil {
		t.Fatal("missing URI source admitted")
	}
}

func TestAuxiliaryNullEmbeddedAdmissionWithoutDiscovery(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{{"writable", "SELECT id FROM records"}, {"malformed", "SELECT FROM ???"}, {"missing", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			resources := resource.New()
			files := fstest.MapFS{}
			if tc.sql != "" {
				files["sql/records.sql"] = &fstest.MapFile{Data: []byte(tc.sql)}
			}
			if err := resources.Register("", files); err != nil {
				t.Fatal(err)
			}
			text := `#package('example.com/auxiliaryadmission/generated')
#setting($_ = $route('/records','PATCH'))
#define($_ = $Rows<[]*RecordsView>(body/data).Cardinality('Many').Required())
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.*,root_null_policy(r,'skip-auxiliary') FROM (${embed:sql/records.sql}) r`
			destination := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/auxiliaryadmission"}).Write(t, destination)
			source := &Source{Scope: "example.com/auxiliaryadmission/generated", Name: "Records", Connector: "main", Text: text, Resources: resources, Types: typecatalog.NewCatalog()}
			_, err := NewCompiler().Transcribe(context.Background(), Request{Source: source, Destination: destination, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}})
			if err == nil {
				t.Fatal("unresolved or nonauxiliary source admitted without discovery")
			}
			if source.Text != text {
				t.Fatal("rejection mutated authored DQL")
			}
			count := 0
			if err := filepath.WalkDir(destination, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() && strings.HasSuffix(path, ".go") {
					count++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("rejected contract emitted %d Go files", count)
			}
		})
	}
}
