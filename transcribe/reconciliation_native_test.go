package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestFiniteReconciliationStockGenerationSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if e := db.ExecStatements(ctx, `CREATE TABLE batches(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,total INTEGER NOT NULL DEFAULT 0)`, `CREATE TABLE entries(id INTEGER PRIMARY KEY AUTOINCREMENT,batch_id INTEGER NOT NULL REFERENCES batches(id),title TEXT NOT NULL)`); e != nil {
		t.Fatal(e)
	}
	binary := filepath.Join(t.TempDir(), "datly")
	build := testharness.SourceGoCommand(t, repoRoot(t), "build", "-mod=readonly", "-o", binary, "./cmd/datly")
	build.Dir = repoRoot(t)
	if output, e := build.CombinedOutput(); e != nil {
		t.Fatalf("CLI build: %v\n%s", e, output)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/finitereconcile"}).Write(t, root)
	source := `#package('example.com/finitereconcile/generated')

#setting($_ = $connector('main'))
#setting($_ = $route('/batches','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))

#define($_ = $Data<[]*Batch>(output/body))

SELECT b.*,e.*,type(b,'Batch'),type(e,'Entry'),lifecycle_type(b,'Hooks'),finite_reconciliation(b,'{"mode":"same-parent-root-first","rootFields":["Total"],"roles":[{"holder":"Entries","fields":["Title","BatchId"],"adoptIdentity":true}]}')
FROM (SELECT batches.* FROM batches) b
JOIN (SELECT entries.* FROM entries) e ON e.batch_id=b.id
`
	// Holder names are explicit, so the role descriptor remains stable when SQL
	// aliases change. This declaration is handled by ordinary SQL-derived shaping.
	source = strings.Replace(source, "type(e,'Entry')", "type(e,'Entry','Entries')", 1)
	writeSourceFile(t, root, "source/Batches.dql", source)
	run := func(wantFailure bool) string {
		t.Helper()
		command := exec.Command(binary, "transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/finitereconcile/source")
		command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on", "GOFLAGS=-mod=readonly")
		output, e := command.CombinedOutput()
		if (e != nil) != wantFailure {
			t.Fatalf("stock transcribe failed=%v wanted=%v\n%s", e, wantFailure, output)
		}
		return string(output)
	}
	run(false)
	generated, e := os.ReadFile(filepath.Join(root, "generated", "input.go"))
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(generated), "finiteReconciliation=") {
		t.Fatal("generated input lost opt-in metadata")
	}
	// Regeneration must validate an authored canonical batch method through stock
	// transcription; this file is application lifecycle code, never generated Go.
	writeSourceFile(t, root, "generated/reconcile_input.go", `package generated
import("context";"github.com/viant/datly/runtime/handler/writer")
func(*Hooks)ReconcileInput(ctx context.Context,input *Input,output *Output,native writer.ReconciliationContext)(writer.ReconciliationPlan,error){
 roots,err:=native.Roots();if err!=nil{return writer.ReconciliationPlan{},err};plan:=writer.ReconciliationPlan{}
 for _,root:=range roots{entry:=writer.ReconciliationRootPlan{Root:root.Ref};for _,role:=range root.Roles{children:=writer.ReconciliationRolePlan{Holder:role.Holder};for _,row:=range role.Working{children.Selected=append(children.Selected,writer.ReconciliationSelection{Occurrence:row.Ref})};entry.Roles=append(entry.Roles,children)};plan.Roots=append(plan.Roots,entry)};return plan,nil
}`)
	run(false)
	body := func() string {
		t.Helper()
		files, e := os.ReadDir(filepath.Join(root, "generated"))
		if e != nil {
			t.Fatal(e)
		}
		for _, file := range files {
			if !strings.HasSuffix(file.Name(), ".go") {
				continue
			}
			data, e := os.ReadFile(filepath.Join(root, "generated", file.Name()))
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(string(data), "type Batch struct") {
				return string(data)
			}
		}
		t.Fatal("generated Batch missing")
		return ""
	}
	initial := body()
	run(false)
	if initial != body() {
		t.Fatal("unchanged schema regenerated a different body")
	}
	if e = db.ExecStatements(ctx, `ALTER TABLE batches ADD COLUMN notes TEXT`); e != nil {
		t.Fatal(e)
	}
	run(false)
	evolved := body()
	if initial == evolved || !strings.Contains(evolved, "Notes") {
		t.Fatal("schema change did not regenerate native body")
	}
	writeSourceFile(t, root, "generated/reconciliation_bootstrap_test.go", reconciliationBootstrapRuntime)
	command := testharness.SourceGoCommand(t, root, "test", "-mod=readonly", "-count=1", "./generated", "-run", "TestGeneratedFiniteReconciliationBootstrap")
	command.Dir = root
	if output, e := command.CombinedOutput(); e != nil {
		t.Fatalf("generated native bootstrap: %v\n%s", e, output)
	} else {
		t.Logf("generated native bootstrap: %s", output)
	}
	malformed := strings.Replace(source, `"Total"`, `"Id"`, 1)
	writeSourceFile(t, root, "source/Batches.dql", malformed)
	if output := run(true); !strings.Contains(output, "finite_reconciliation") {
		t.Fatal("missing declaration diagnostic", output)
	}
	writeSourceFile(t, root, "source/Batches.dql", source)
	for _, policy := range []string{"writer_identity(b,'assigned-update')", "queue_contract(b,'source-row')", "writer_action_policy(e,'insert-delete')"} {
		writeSourceFile(t, root, "source/Batches.dql", strings.Replace(source, "SELECT b.*", fmt.Sprintf("SELECT %s,b.*", policy), 1))
		run(true)
	}
}

const reconciliationBootstrapRuntime = `package generated
import(
 "context"
 "database/sql"
 "path/filepath"
 "net/http/httptest"
 requestprovider "github.com/viant/bindly/provider/request"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
 "github.com/viant/bindly/locator"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/runtime/handler/engine"
 "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/datly/tag"
)
func TestGeneratedFiniteReconciliationBootstrap(t *testing.T){
 holder:=reflect.TypeFor[BatchesComponent]();field,_:=holder.FieldByName("Contract")
 metadata,present,err:=tag.ParseComponent(field.Tag);if err!=nil || !present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 if component.RootView.Reconciliation==nil || component.RootView.Reconciliation.Mode!="same-parent-root-first"{t.Fatal("bootstrap lost reconciliation metadata")}
 for _,view:=range component.Views {if view!=component.RootView && view.Reconciliation!=nil{t.Fatal("Current reader retained reconciliation")}}
 resources:=resource.New();if err=resources.Register(BatchesDatlyResourceNamespace,BatchesDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 db,err:=sql.Open("sqlite3",filepath.Join(t.TempDir(),"runtime.db"));if err!=nil{t.Fatal(err)};defer db.Close()
 for _,statement:=range []string{"CREATE TABLE batches(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,total INTEGER NOT NULL DEFAULT 0,notes TEXT)","CREATE TABLE entries(id INTEGER PRIMARY KEY AUTOINCREMENT,batch_id INTEGER NOT NULL REFERENCES batches(id),title TEXT NOT NULL)","INSERT INTO batches(id,title) VALUES(1,'existing')"}{if _,err=db.Exec(statement);err!=nil{t.Fatal(err)}}
 request:=httptest.NewRequest("PATCH","/batches",strings.NewReader("{\"Data\":[{\"id\":1,\"title\":\"updated\",\"Entries\":[{\"title\":\"new entry\"}]}]}"));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 route,_:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/batches"})
 _,err=engine.New().Execute(context.Background(),engine.Request{Input:route,Scope:scope,Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}});if err!=nil{t.Fatal(err)}
 var title string;var parent int;if err=db.QueryRow("SELECT title,batch_id FROM entries").Scan(&title,&parent);err!=nil || title!="new entry" || parent!=1{t.Fatal("native generated persistence",title,parent,err)}
}
`
