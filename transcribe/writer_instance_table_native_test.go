package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

// Generate once against the default schema, then execute the emitted contracts
// with two instances. Discovery must not freeze the default physical DML table.
func TestGeneratedWriterInstanceTableSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL,region INTEGER NOT NULL,PRIMARY KEY(id,region))"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/instancewriter"}).Write(t, root)
	source := "#package('example.com/instancewriter/generated')\n" +
		"#setting($_ = $connector('main'))\n#setting($_ = $route('/records','POST'))\n" +
		"#setting($_ = $input_type('Input'))\n#setting($_ = $output_type('Output'))\n" +
		"#define($_ = $Table<string>(const/Table).Value('records'))\n" +
		"#define($_ = $Data<[]*Record>(output/body))\n" +
		"SELECT r.*,type(r,'Record') FROM (SELECT id,region FROM `${Table}`) r"
	if _, err := (Generator{Operation: "post"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "example.com/instancewriter/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}); err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(root, "generated", "input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "${Table}") {
		t.Fatalf("discovery froze writer table: %s", input)
	}
	writeSourceFile(t, root, "generated/instance_runtime_test.go", writerInstanceTableRuntime)
	command := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=go1.25.8")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated instance writer: %v\n%s", err, output)
	}
}

const writerInstanceTableRuntime = `package generated
import (
 "context"
 "database/sql"
 "path/filepath"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/datly/bootstrap/connector"
 "github.com/viant/datly/constant"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
)
func TestInstanceTargets(t *testing.T) {
 for _,target:=range []string{"records","soak_records","records; DROP TABLE records"}{
  t.Run(target,func(t *testing.T){
   ctx:=context.Background();dsn:=filepath.Join(t.TempDir(),"records.db")
   db,err:=sql.Open("sqlite3",dsn);if err!=nil{t.Fatal(err)};defer db.Close()
   for _,statement:=range []string{"CREATE TABLE records(id INTEGER,region INTEGER,PRIMARY KEY(id,region))","CREATE TABLE soak_records(id INTEGER,region INTEGER,PRIMARY KEY(id,region))"}{if _,err=db.Exec(statement);err!=nil{t.Fatal(err)}}
   values,err:=constant.New(map[string]string{"Table":target});if err!=nil{t.Fatal(err)}
   server,err:=standalone.New(ctx,standalone.Options{RequireLinked:true,Config:&config.Config{Const:values,Endpoint:config.Endpoint{Address:"127.0.0.1:0"},GoBootstrap:&config.Packages{Packages:[]string{"example.com/instancewriter/generated"},LinkedOnly:true},Connector:"main",Connectors:[]connector.Config{{Name:"main",Driver:"sqlite3",DSN:dsn}}}});if err!=nil{t.Fatal(err)}
   defer server.Shutdown(ctx);if err=server.Reload(ctx,1);err!=nil{t.Fatal(err)}
   id,region:=1,2;row:=&Record{};row.SetId(&id);row.SetRegion(&region)
   input:=&Input{};input.SetRecords([]*Record{row});input.SetTable("records")
   invoke:=func(in *Input)error{_,e:=server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"example.com/instancewriter/generated",Name:"Records"},Route:spec.RouteRef{Method:"POST",Path:"/records"}},Input:in});return e}
   err=invoke(input);invalid:=target=="records; DROP TABLE records"
   if invalid{if err==nil{t.Fatal("invalid identifier accepted")}}else{if err!=nil{t.Fatal(err)}}
   if !invalid{
    id2,region2:=2,1;added:=&Record{};added.SetId(&id2);added.SetRegion(&region2)
    duplicate:=&Record{};duplicate.SetId(&id);duplicate.SetRegion(&region)
    failed:=&Input{};failed.SetRecords([]*Record{added,duplicate})
    if err=invoke(failed);err==nil{t.Fatal("duplicate composite key accepted")}
   }
   for _,table:=range []string{"records","soak_records"}{var count int;if err=db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count);err!=nil{t.Fatal(err)};want:=0;if table==target{want=1};if count!=want{t.Fatalf("target=%s table=%s rows=%d want=%d",target,table,count,want)}}
  })
 }
}
`
