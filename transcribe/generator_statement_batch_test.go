package transcribe

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"

	loaderast "github.com/viant/x/loader/ast"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGeneratorBufferedSQLXBatchContract(t *testing.T) {
	for _, method := range []string{"Insert", "Delete"} {
		t.Run(method, func(t *testing.T) { testGeneratedSQLXBatch(t, method) })
	}
}

func testGeneratedSQLXBatch(t *testing.T, method string) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "model"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "model", "record.go"), []byte("package model\ntype Record struct { ID int `sqlx:\"ID,primaryKey\"`; Name string `sqlx:\"NAME\"` }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	const source = `#package('pkg/records')
#import('model','github.com/viant/datly/genfixture/model')
#setting($_ = $route('/records/batch','POST'))
#define($_ = $Table<string>(param/table))
#define($_ = $Rows<[]*model.Record>(param/rows))
#define($_ = $BatchSize<int>(query/batchSize).Value('8000'))
#define($_ = $Affected<int64>(output/body))
$dml.Insert($Input.Table, $Input.Rows, $Input.BatchSize, $Output.Affected)`
	catalog := typecatalog.NewCatalog()
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: strings.Replace(source, "$dml.Insert(", "$dml."+method+"(", 1), Types: catalog}}
	generated, err := (Generator{Operation: "post"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("Batch acquired a competing writer")
	}
	p := filepath.Join(root, "pkg/records", "handler.go")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), method+"WithBatchSize(input.Table, input.Rows, input.BatchSize, &output.Affected)") {
		t.Fatalf("native batch was not generated: %s", before)
	}
	if _, err := (Generator{Operation: "post"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(p)
	if err != nil || string(before) != string(after) {
		t.Fatal("Batch generation is unstable")
	}
	runtime := `package records
import("context";"encoding/json";"reflect";"testing";
"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/runtime/handler/custom";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/sql/dml";"github.com/viant/datly/internal/testharness/sqlite";
"github.com/viant/datly/genfixture/model")
func TestNativeGeneratedBatch(t *testing.T){
 ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE records(ID INTEGER PRIMARY KEY,NAME TEXT)");err!=nil{t.Fatal(err)}
 var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
 handler:=custom.New(NewRecordsHandler());artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:&component,InputType:reflect.TypeFor[RecordsInput](),OutputType:reflect.TypeFor[RecordsOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
 route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/records/batch"});if !ok{t.Fatal("missing route")}
 for _,failure:=range []bool{false,true}{
  if failure{if err:=h.ExecStatements(ctx,"CREATE TRIGGER fail_tail BEFORE INSERT ON records WHEN NEW.ID=3 BEGIN SELECT RAISE(ABORT,'tail failure'); END");err!=nil{t.Fatal(err)}}
  rows:=[]*model.Record{{ID:1,Name:"one"},{ID:2,Name:"two"},{ID:3,Name:"three"}}
  input:=&RecordsInput{Table:"records",Rows:rows};input.SetBatchSize(2)
  value,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:handler,DataSource:dml.Source{DB:h.DB},BoundInput:input})
  if failure{if err==nil{t.Fatal("failed batch reported success")};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{0}})
  }else{if err!=nil{t.Fatal(err)};out,ok:=value.(*RecordsOutput);if !ok||out.Affected!=3{t.Fatalf("result=%+v",value)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{3}});if err:=h.ExecStatements(ctx,"DELETE FROM records");err!=nil{t.Fatal(err)}}
 }
}
`
	if method == "Delete" {
		runtime = strings.Replace(runtime, "for _,failure:=range []bool{false,true}{", "for _,failure:=range []bool{false,true}{ if err:=h.ExecStatements(ctx,\"INSERT INTO records(ID,NAME) VALUES(1,'one'),(2,'two'),(3,'three')\");err!=nil{t.Fatal(err)}", 1)
		runtime = strings.ReplaceAll(runtime, "BEFORE INSERT ON records WHEN NEW.ID=3", "BEFORE DELETE ON records WHEN OLD.ID=3")
		runtime = strings.Replace(runtime, "[]struct{Count int}{{0}})", "[]struct{Count int}{{3}})", 1)
		// The committed delete leaves the table empty; the failed second chunk restores all keys.
		marker := ";h.AssertQuery(t,ctx,sqlite.Query{SQL:\"SELECT COUNT(*) AS count FROM records\"},[]struct{Count int}{{3}});if err:="
		runtime = strings.Replace(runtime, marker, strings.Replace(marker, "{{3}}", "{{0}}", 1), 1)
	}
	compiled, err := NewCompiler().Compile(context.Background(), request.Source)
	if err != nil {
		t.Fatal(err)
	}
	componentJSON, err := json.Marshal(compiled.Component)
	if err != nil {
		t.Fatal(err)
	}
	runtime = strings.ReplaceAll(runtime, "RecordsInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "RecordsOutput", generated.Result.Plan.Output.Type)
	runtime = strings.ReplaceAll(runtime, "COMPONENT_JSON", strconv.Quote(string(componentJSON)))
	if err := os.WriteFile(filepath.Join(root, "pkg/records/batch_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated batch owner regression: %v\n%s", err, output)
	}

}
