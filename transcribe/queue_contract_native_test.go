package transcribe

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/viant/datly/internal/testharness"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueueContractStockCLIAndSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT)`); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "datly")
	build := testharness.SourceGoCommand(t, repoRoot(t), "build", "-o", binary, "./cmd/datly")
	build.Dir = repoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("CLI build: %v\n%s", err, output)
	}
	for _, tc := range []struct{ operation, contract string }{{"post", "source-row"}, {"patch", "source-row"}, {"post", "source-slice"}, {"patch", "source-slice"}} {
		operation, contract := tc.operation, tc.contract
		t.Run(operation+"/"+contract, func(t *testing.T) {
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT)`); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/queuecontract"}).Write(t, root)
			text := fmt.Sprintf(`#package('example.com/queuecontract/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','%s'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.*,type(r,'Record'),queue_contract(r,'%s')
FROM (SELECT records.* FROM records) r
`, strings.ToUpper(operation), contract)
			writeSourceFile(t, root, "source/Records.dql", text)
			run := func() {
				t.Helper()
				command := exec.Command(binary, "transcribe", operation, "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/queuecontract/source")
				command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
				out, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("stock transcribe %s: %v\n%s", operation, err, out)
				}
			}
			snapshot := func() map[string][32]byte {
				t.Helper()
				result := map[string][32]byte{}
				err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !entry.IsDir() && strings.HasSuffix(path, ".go") {
						data, e := os.ReadFile(path)
						if e != nil {
							return e
						}
						result[path] = sha256.Sum256(data)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			run()
			first := snapshot()
			run()
			second := snapshot()
			if len(first) != len(second) {
				t.Fatal("unstable generated file count")
			}
			for p, digest := range first {
				if second[p] != digest {
					t.Fatal("unstable regeneration", p)
				}
			}
			found := false
			for p := range first {
				data, _ := os.ReadFile(p)
				if strings.Contains(string(data), "queueContract="+contract) {
					found = true
				}
				if strings.Contains(filepath.Base(p), "current") && strings.Contains(string(data), "queueContract=") {
					t.Fatal("Current reader retained queue contract", p)
				}
			}
			if !found {
				t.Fatal("generated native queue contract missing")
			}
			if err := db.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN notes TEXT"); err != nil {
				t.Fatal(err)
			}
			run()
			evolved := snapshot()
			changed := false
			for p, digest := range evolved {
				if first[p] != digest {
					changed = true
				}
			}
			if !changed {
				t.Fatal("fixture schema evolution did not regenerate body")
			}
			runtime := strings.ReplaceAll(queueContractBootstrapRuntime, `"source-row"`, fmt.Sprintf("%q", contract))
			if contract == "source-slice" {
				runtime = strings.ReplaceAll(runtime, "queue_contract source-row does not support update", "queue_contract source-slice requires INSERT in a collection holder")
			}
			writeSourceFile(t, root, "generated/queue_contract_bootstrap_test.go", runtime)
			command := testharness.SourceGoCommand(t, root, "test", "-mod=mod", "-race", "-count=1", "-v", "./generated", "-run", "TestGeneratedQueueContractBootstrap")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated native bootstrap: %v\n%s", err, output)
			} else {
				t.Logf("generated native runtime:\n%s", output)
			}
		})
	}
	for _, tc := range []struct{ name, operation, expression string }{
		{"read-only", "get", "queue_contract(r,'source-row')"},
		{"update-role", "put", "queue_contract(r,'source-row')"},
		{"slice-update-role", "put", "queue_contract(r,'source-slice')"},
		{"unknown", "post", "queue_contract(r,'unknown')"},
		{"duplicate", "post", "queue_contract(r,'source-row'),queue_contract(r,'source-row')"},
		{"aliased", "post", "queue_contract(r,'source-row') AS improper"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/queuecontract"}).Write(t, root)
			sentinel := filepath.Join(root, "generated", "sentinel.go")
			writeSourceFile(t, root, "generated/sentinel.go", "package generated\n// preserved authoring destination\n")
			before, _ := os.ReadFile(sentinel)
			source := fmt.Sprintf("#package('example.com/queuecontract/generated')\n#setting($_ = $connector('main'))\n#setting($_ = $route('/records','%s'))\nSELECT r.*,type(r,'Record'),%s FROM (SELECT records.* FROM records) r\n", strings.ToUpper(tc.operation), tc.expression)
			writeSourceFile(t, root, "source/Records.dql", source)
			command := exec.Command(binary, "transcribe", tc.operation, "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/queuecontract/source")
			command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "queue_contract") {
				t.Fatalf("unsupported stock authoring admitted: %v\n%s", err, output)
			}
			after, _ := os.ReadFile(sentinel)
			if string(after) != string(before) {
				t.Fatal("failed authoring altered existing destination")
			}
			entries, err := os.ReadDir(filepath.Dir(sentinel))
			if err != nil || len(entries) != 1 {
				t.Fatal("failed authoring published files", entries, err)
			}
		})
	}

}

const queueContractBootstrapRuntime = `package generated
import (
 "context"
 "database/sql"
 "errors"
 "net/http/httptest"
 "path/filepath"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/runtime/handler/engine"
 "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/datly/tag"
 xhandler "github.com/viant/xdatly/handler"
)
func TestGeneratedQueueContractBootstrap(t *testing.T) {
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract")
 metadata,present,err:=tag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"Input",OutputType:"Output"}
 c,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 if c.RootView.QueueContract!="source-row"{t.Fatal("bootstrap lost authored queue contract")}
 operation:=c.Settings.Mutation
 for _,v:=range c.Views {if v!=c.RootView && v.QueueContract!=""{t.Fatal("reader retained writer queue contract")}}
 resources:=resource.New()
 if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:c,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 for _,mode:=range []string{"success-owned","success-caller","unique-owned","unique-caller","update-rejection","cancelled-context"}{
  if mode=="update-rejection"&&operation!="patch"{continue}
  t.Run(operation+"/"+mode,func(t *testing.T){
   base:=context.Background();db,err:=sql.Open("sqlite3",filepath.Join(t.TempDir(),"runtime.db"));if err!=nil{t.Fatal(err)};defer db.Close()
   for _,q:=range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT UNIQUE,notes TEXT,details TEXT)","CREATE TABLE audit(id INTEGER)","CREATE TRIGGER added AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(new.id);END"}{if _,err=db.Exec(q);err!=nil{t.Fatal(err)}}
   if mode=="update-rejection"{if _,err=db.Exec("INSERT INTO records(id,title) VALUES(10,'existing');DELETE FROM audit");err!=nil{t.Fatal(err)}}
   var tx *sql.Tx
   if strings.HasSuffix(mode,"caller"){tx,err=db.BeginTx(base,nil);if err!=nil{t.Fatal(err)};defer tx.Rollback()}
   var providers []locator.Provider
   if len(artifact.ViewDependencies)>0{views,e:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db,Tx:tx}});if e!=nil{t.Fatal(e)};providers=append(providers,views)}
   native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),operation);if err!=nil{t.Fatal(err)}
   route:=c.Routes[0];binding,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:route.Method,Path:route.Path});if !ok{t.Fatal("generated route binding missing")}
   body:="{\"Data\":[{\"title\":\"one\"},{\"title\":\"two\"}]}"
   if strings.HasPrefix(mode,"unique"){body="{\"Data\":[{\"title\":\"duplicate\"},{\"title\":\"duplicate\"}]}"}
   if mode=="update-rejection"{body="{\"Data\":[{\"id\":10,\"title\":\"must reject\"}]}"}
   req:=httptest.NewRequest(route.Method,route.Path,strings.NewReader(body));req.Header.Set("Content-Type","application/json")
   scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)};defer scope.Close()
   ctx:=base
   if mode=="cancelled-context"{cancelled,cancel:=context.WithCancel(base);cancel();defer cancel();ctx=cancelled}
   commits:=0;var outcome xhandler.Outcome
   out,executionErr:=engine.New().Execute(ctx,engine.Request{Input:binding,Handler:native,Scope:scope,Providers:providers,DataSource:dml.Source{DB:db,Tx:tx,OnCommit:func(context.Context){commits++}},Completion:func(o xhandler.Outcome){outcome=o}})
   want:=2
   if strings.HasPrefix(mode,"unique"){if executionErr==nil||!strings.Contains(executionErr.Error(),"UNIQUE"){t.Fatal("real late SQL failure required",executionErr)};want=0;if tx!=nil && c.RootView.QueueContract!="source-slice"{want=1}}
   if mode=="update-rejection"{if executionErr==nil||!strings.Contains(executionErr.Error(),"queue_contract source-row does not support update"){t.Fatal("UPDATE must reject by queue action admission",executionErr)};want=1;var title string;if err=db.QueryRow("SELECT title FROM records WHERE id=10").Scan(&title);err!=nil||title!="existing"{t.Fatal("UPDATE altered original",title,err)};var seq int;if err=db.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&seq);err!=nil||seq!=10{t.Fatal("UPDATE rejection advanced allocator",seq,err)};var infra int;if err=db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'").Scan(&infra);err!=nil||infra!=0{t.Fatal("UPDATE rejection allocated",infra,err)}}
   if mode=="cancelled-context"{if ctx.Err()!=context.Canceled||!errors.Is(executionErr,context.Canceled){t.Fatal("true context cancellation missing",executionErr)};want=0}
   if strings.HasPrefix(mode,"success"){
    if executionErr!=nil{t.Fatal(executionErr)}
    output,ok:=out.(*Output);if !ok{t.Fatal("native typed output missing",out)}
    rows:=reflect.ValueOf(output).Elem().FieldByName("Data");if rows.Len()!=2{t.Fatal("native output row count",rows.Len())}
    ids:=map[int64]bool{}
    for i:=0;i<rows.Len();i++{row:=rows.Index(i).Elem();for j:=0;j<row.NumField();j++{if strings.HasPrefix(row.Type().Field(j).Tag.Get("sqlx"),"id,"){v:=row.Field(j);if v.Kind()==reflect.Pointer{v=v.Elem()};id:=v.Int();if id<=0||ids[id]{t.Fatal("public native allocation lost",id)};ids[id]=true}}};if len(ids)!=2{t.Fatal("identity evidence missing")}
    if tx!=nil{if commits!=0||outcome.State()!=xhandler.TransactionCallerPending{t.Fatal("caller ownership lost",commits,outcome.State())}}else if commits!=1{t.Fatal("owned commit missing",commits)}
   }else if commits!=0{t.Fatal("failure committed",commits)}
   query:=db.QueryRow;if tx!=nil{query=tx.QueryRow}
   for _,table:=range []string{"records","audit"}{var n int;expected:=want;if mode=="update-rejection"&&table=="audit"{expected=0};if err=query("SELECT COUNT(*) FROM "+table).Scan(&n);err!=nil||n!=expected{t.Fatal("physical effects",table,n,expected,err)}}
   if tx!=nil{if _,err=tx.Exec("INSERT INTO records(id,title) VALUES(999,'caller-usable')");err!=nil{t.Fatal(err)};if err=tx.Rollback();err!=nil{t.Fatal(err)};for _,table:=range []string{"records","audit"}{var n int;if err=db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n);err!=nil||n!=0{t.Fatal("caller rollback failed",table,n,err)}}}
  })
 }
}
`
