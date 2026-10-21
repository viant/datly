package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratorNestedStatementResultSQLite(t *testing.T) {
	for _, method := range []string{"ExecuteWithResult", "ExecuteWithOptionalResult"} {
		t.Run(method, func(t *testing.T) { testGeneratorNestedStatementResultSQLite(t, method) })
	}
}
func testGeneratorNestedStatementResultSQLite(t *testing.T, method string) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "model"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "model/row.go"), []byte("package model\ntype Row struct { Id int; Name string; Next *Row; Metadata *string }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	source := `#package('pkg/records')
#import('shared','github.com/viant/datly/genfixture/model')
#setting($_ = $route('/records','POST'))
#define($_ = $Request<*shared.Row>(body/data).Required())
#define($_ = $Row<*shared.Row>(output/body))
$dml.ExecuteWithResult("INSERT INTO records(name, metadata) VALUES (?, ?)", $Output.Row.Id, $Input.Request.Name, $Input.Request.Metadata)`
	source = strings.ReplaceAll(source, "ExecuteWithResult", method)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source, Types: catalog}}
	generated, err := (Generator{Operation: "post"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil {
		t.Fatal("nested statement lost sole persistence owner")
	}
	path := filepath.Join(root, "pkg/records/handler.go")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), "&output.Row.Id") || !strings.Contains(string(before), "output.Row == nil") || !strings.Contains(string(before), "input.Request == nil") {
		t.Fatalf("missing direct nested destination/guards: %s", before)
	}
	if _, err = (Generator{Operation: "post"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("nested statement generation unstable")
	}
	for _, file := range []string{"input.go", "output.go"} {
		b, err := os.ReadFile(filepath.Join(root, "pkg/records", file))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "type Row struct") {
			t.Fatal("shared row duplicated")
		}
	}
	const runtime = `package records
import("context";"errors";"strings";"testing";"github.com/viant/datly/genfixture/model";"github.com/viant/datly/internal/testharness/sqlite";"github.com/viant/datly/sql/dml")
func TestNestedResultCallerDrain(t *testing.T){ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,metadata TEXT)");err!=nil{t.Fatal(err)}
 for _,name:=range []string{"saved","blocked","caller aborted"}{
  if name=="blocked" {if err:=h.ExecStatements(ctx,"CREATE TRIGGER reject_blocked BEFORE INSERT ON records WHEN NEW.name='blocked' BEGIN SELECT RAISE(ABORT,'blocked record'); END");err!=nil{t.Fatal(err)}}
  row:=&model.Row{Name:name,Id:-1};in:=&RecordsInput{Request:row};out:=&RecordsOutput{Row:row};data:=dml.NewData(h.DB);if err:=data.BeginInvocation();err!=nil{t.Fatal(err)}
  if err:=NewRecordsHandlerQueueStatement(data,in,out);err!=nil{t.Fatal(err)}
  if out.Row!=row||row.Id!=-1{t.Fatal("queue replaced row or assigned eagerly")}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{map[bool]int{true:0,false:1}[name=="saved"]}})
  var failure error;if name=="caller aborted"{failure=errors.New("caller aborted")}
  err:=data.Complete(ctx,failure)
  if name=="saved" {if err!=nil||row.Id!=1||out.Row!=row||in.Request!=row||row.Name!=name||row.Metadata!=nil{t.Fatalf("identity/default/null changed: %+v %v",out,err)}} else if err==nil||row.Id!=-1||out.Row!=row{t.Fatalf("failed drain lost identity/rollback: %+v %v",out,err)}
 }
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name,metadata FROM records"},[]struct{ID int;Name string;Metadata *string}{{1,"saved",nil}})
 for _,pair:=range []struct{in *RecordsInput;out *RecordsOutput;want string}{{&RecordsInput{},&RecordsOutput{},"Output.Row"},{&RecordsInput{},&RecordsOutput{Row:&model.Row{}},"Input.Request"}}{
  data:=dml.NewData(h.DB);if err:=data.BeginInvocation();err!=nil{t.Fatal(err)};err:=NewRecordsHandlerQueueStatement(data,pair.in,pair.out);if err==nil||!strings.Contains(err.Error(),pair.want){t.Fatalf("missing focused nil diagnostic: %v",err)};if err:=data.Complete(ctx,nil);err!=nil{t.Fatal(err)}
 }
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{1}})
}`
	if err = os.WriteFile(filepath.Join(root, "pkg/records/nested_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated nested native statement: %v\n%s", err, output)
	}
}
