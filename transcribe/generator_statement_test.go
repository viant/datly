package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratorBufferedStatementNoBodySQLite(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	const source = `#package('pkg/records')
#setting($_ = $route('/records/{name}', 'PATCH'))

#define($_ = $Name<string>(path/name))
#define($_ = $Identity<int64>(output/body))

$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Output.Identity, $Input.Name)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source}}
	compiled, err := NewCompiler().Compile(context.Background(), request.Source)
	if err != nil {
		t.Fatal(err)
	}
	componentJSON, err := json.Marshal(compiled.Component)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil {
		t.Fatal("statement acquired a competing persistence owner")
	}
	if generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("statement contract advertised an automatic record writer")
	}
	for _, field := range generated.Result.Plan.Input.Fields {
		if strings.Contains(field.Tag, "body") {
			t.Fatal("no-body writer acquired a body input")
		}
	}
	pkg := filepath.Join(root, "pkg/records")
	before, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Generator{Operation: "patch"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil || string(before) != string(after) {
		t.Fatal("statement generation is unstable")
	}
	runtime := `package records
import("context";"encoding/json";"fmt";"reflect";"testing";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values";"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/internal/testharness/sqlite";"github.com/viant/datly/runtime/handler/custom";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/sql/dml")
func(o *RecordsOutput)Finalize(context.Context)error{if o.Identity<=0{return fmt.Errorf("finalizer ran before statement identity assignment")};return nil}
func TestNativeStatement(t *testing.T){ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)");err!=nil{t.Fatal(err)}
var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
handler:=custom.New(NewRecordsHandler());artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:&component,InputType:reflect.TypeFor[RecordsInput](),OutputType:reflect.TypeFor[RecordsOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/records/{name}"});if !ok{t.Fatal("route not compiled")}
result,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:handler,DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{values.New("path",map[string]any{"name":"record"})}})
if err!=nil{t.Fatal(err)};output,ok:=result.(*RecordsOutput);if !ok||output.Identity!=1{t.Fatalf("result=%+v",result)}
h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{ID int;Name string}{{1,"record"}})
if err:=h.ExecStatements(ctx,"CREATE TRIGGER reject_blocked BEFORE INSERT ON records WHEN NEW.name='blocked' BEGIN SELECT RAISE(ABORT,'blocked record'); END");err!=nil{t.Fatal(err)}
_,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:handler,DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{values.New("path",map[string]any{"name":"blocked"})}});if err==nil{t.Fatal("failed statement reported success")}
h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{ID int;Name string}{{1,"record"}})
}`
	// Generated-module helpers allow its generic regression to access the same
	// internal SQLite harness without introducing any application dependency.
	runtime = strings.ReplaceAll(runtime, "RecordsInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "RecordsOutput", generated.Result.Plan.Output.Type)
	runtime = strings.ReplaceAll(runtime, "COMPONENT_JSON", strconv.Quote(string(componentJSON)))
	if err := os.WriteFile(filepath.Join(pkg, "statement_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical generated buffered statement: %v\n%s", err, output)
	}
}

func TestGeneratorBufferedStatementSQLShapeEvolution(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	const source = `#package('pkg/records')
#setting($_ = $route('/records/{name}', 'PATCH'))
#define($_ = $Name<string>(path/name))
#define($_ = $Identity<int64>(output/identity))
#define($_ = $Data<*RecordsView>(output/body))

SELECT record.* FROM records record;
$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Output.Identity, $Input.Name)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source, ColumnRefiner: column.New(column.Connections{"main": h.DB})}}
	before, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if before.Result.Plan.ContractHandler == nil {
		t.Fatal("shape SQL replaced statement handler")
	}
	compiled, err := NewCompiler().Compile(ctx, request.Source)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(compiled.Component.RootView.Source.SQL, "$dml") {
		t.Fatal("queued write leaked into the shape discovery source")
	}
	if err := h.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN note TEXT"); err != nil {
		t.Fatal(err)
	}
	after, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, view := range after.Result.Plan.Views {
		for _, field := range view.Fields {
			if field.Name == "Note" {
				found = true
				if field.Type != "*string" {
					t.Fatalf("new nullable column type %s", field.Type)
				}
			}
		}
	}
	if !found {
		t.Fatal("SQL wildcard did not evolve generated output shape")
	}
	for _, field := range after.Result.Plan.Input.Fields {
		if strings.Contains(field.Tag, "body") {
			t.Fatal("SQL shape invented a request body")
		}
	}
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("evolved generated output: %v\n%s", err, output)
	}
}

func TestGeneratorBufferedStatementDerivedShapeRetainsPhysicalMetadata(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE owners(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,owner_id INTEGER NOT NULL REFERENCES owners(id))"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	const source = `#package('pkg/records')
#setting($_ = $route('/records/{ownerId}', 'PATCH'))
#define($_ = $OwnerId<int>(path/ownerId))
#define($_ = $Identity<int64>(output/identity))
#define($_ = $Data<*RecordsView>(output/body))
SELECT record.*, CAST(record.id AS int), CAST(record.owner_id AS int)
FROM (SELECT id, owner_id FROM records) record;
$dml.ExecuteWithResult("INSERT INTO records(owner_id) VALUES (?)", $Output.Identity, $Input.OwnerId)`
	result, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source, ColumnRefiner: column.New(column.Connections{"main": h.DB})})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range result.Component.RootView.Columns {
		switch strings.ToLower(c.Name) {
		case "id":
			if !c.PrimaryKey {
				t.Fatalf("derived shape lost physical identity: %+v", c)
			}
		case "owner_id":
			if !strings.Contains(c.Tag, "refTable=owners") {
				t.Fatalf("derived shape lost physical foreign key: %+v", c)
			}
		}
	}
}

func TestGeneratorBufferedStatementRejectsCompetingPoliciesAndTargets(t *testing.T) {
	const source = `#package('pkg/records')
#setting($_ = $route('/records/{name}', 'PATCH'))
#define($_ = $Name<string>(path/name))
#define($_ = $Identity<int64>(output/body))
$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Output.Identity, $Input.Name)`
	for _, name := range []string{"reader", "velty", "sequence", "identity", "queue", "action", "hooks"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
			compiled, err := NewCompiler().Compile(context.Background(), &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Text: source})
			if err != nil {
				t.Fatal(err)
			}
			generator := Generator{Operation: "patch"}
			if compiled.Component.Settings == nil {
				compiled.Component.Settings = &spec.Settings{}
			}
			if compiled.Component.RootView == nil {
				compiled.Component.RootView = &spec.View{}
			}
			switch name {
			case "reader":
				generator.Operation = "get"
			case "velty":
				generator.Language = HandlerVelty
			case "sequence":
				compiled.Component.Settings.SequenceStrategy = "transient"
			case "identity":
				compiled.Component.RootView.WriterIdentityPolicy = "source"
			case "queue":
				compiled.Component.RootView.QueueContract = "row"
			case "action":
				compiled.Component.RootView.WriterActionPolicy = "insert-delete"
			case "hooks":
				compiled.Component.RootView.EntityHooks = "Hooks"
			}
			if _, err := generator.Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root}); err == nil {
				t.Fatal("incompatible statement program accepted")
			}
			if _, err := os.Stat(filepath.Join(root, "pkg/records/handler.go")); !os.IsNotExist(err) {
				t.Fatal("rejected authoring wrote handler artifacts")
			}
		})
	}
}

func TestCompilerBufferedStatementRejectsMixedPrograms(t *testing.T) {
	const write = `$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Output.Identity, $Input.Name)`
	for _, program := range []string{
		"SELECT record.* FROM records record; SELECT other.* FROM records other;\n" + write,
		"SELECT record.* FROM records record;\n" + write + ";\n" + write,
		"SELECT record.* FROM records record;\n$dml.Execute(\"DELETE FROM records\")",
	} {
		_, err := NewCompiler().Compile(context.Background(), &Source{Name: "Records", Text: program})
		if err == nil {
			t.Fatalf("mixed read/write ownership accepted: %s", program)
		}
	}
}
