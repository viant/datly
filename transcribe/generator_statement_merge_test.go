package transcribe

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/metadata/info"
	x "github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestGeneratorBufferedSQLXMergeContract(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "model"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "model", "record.go"), []byte("package model\ntype Record struct { ID int `sqlx:\"ID\"`; Name string `sqlx:\"NAME\"` }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	const source = `#package('pkg/records')
#import('model','github.com/viant/datly/genfixture/model')
#import('info','github.com/viant/sqlx/metadata/info')
#setting($_ = $route('/records/merge','POST'))
#define($_ = $Table<string>(param/table))
#define($_ = $Rows<[]*model.Record>(param/rows))
#define($_ = $Config<info.MergeConfig>(param/config))
#define($_ = $Result<info.MergeResult>(output/body))
$dml.Merge($Input.Table, $Input.Rows, $Input.Config, $Output.Result)`
	catalog := typecatalog.NewCatalog()
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[info.MergeConfig](), reflect.TypeFor[info.MergeResult]()} {
		if err = catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source, Types: catalog}}
	generated, err := (Generator{Operation: "post"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("Merge acquired a competing writer")
	}
	p := filepath.Join(root, "pkg/records", "handler.go")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), "Merge(input.Table, input.Rows, input.Config, &output.Result)") {
		t.Fatalf("native Merge was not generated: %s", before)
	}
	if _, err := (Generator{Operation: "post"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(p)
	if err != nil || string(before) != string(after) {
		t.Fatal("Merge generation is unstable")
	}
	runtime := `package records
import("context";"database/sql";"encoding/json";"fmt";"reflect";"testing";
"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/runtime/handler/custom";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/sql/dml";"github.com/viant/datly/internal/testharness/sqlite";
"github.com/viant/datly/genfixture/model";"github.com/viant/sqlx/io";"github.com/viant/sqlx/metadata/info";"github.com/viant/sqlx/metadata/product/mysql/merge/metric";"github.com/viant/sqlx/metadata/registry";"github.com/viant/sqlx/moption")
type mergeProbe struct{expected []*model.Record;calls int;fail bool}
func(*mergeProbe)DummyMergerConfigFn(){}
func(p *mergeProbe)Exec(ctx context.Context,rows any,db *sql.DB,table string,options ...moption.Option)(info.MergeResult,error){
 p.calls++;got,ok:=rows.([]*model.Record);if !ok||len(got)!=1||got[0]!=p.expected[0]||table!="records"{return nil,fmt.Errorf("merge source replaced")}
 tx:=moption.NewOptions(options...).GetTransaction();if tx==nil{return nil,fmt.Errorf("owner transaction missing")}
 if _,err:=tx.ExecContext(ctx,"INSERT INTO records(id,name) VALUES(?,?)",got[0].ID,got[0].Name);err!=nil{return nil,err}
 result:=&metric.Metric{ToInsertCnt:1};if p.fail{return result,fmt.Errorf("probe failure after insert")};return result,nil
}
func(o *RecordsOutput)Finalize(context.Context)error{if o.Result==nil{return fmt.Errorf("finalizer preceded buffered merge result")};return nil}
func TestNativeGeneratedMerge(t *testing.T){
 registry.RegisterMergeExecutorResolver(func(_ *info.Dialect,c info.MergeConfig)(io.MergeExecutor,error){p,ok:=c.(*mergeProbe);if !ok{return nil,fmt.Errorf("wrong config")};return p,nil},"SQLite")
 ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)");err!=nil{t.Fatal(err)}
 var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
 handler:=custom.New(NewRecordsHandler());artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:&component,InputType:reflect.TypeFor[RecordsInput](),OutputType:reflect.TypeFor[RecordsOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
 route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/records/merge"});if !ok{t.Fatal("missing route")}
 for _,failure:=range []bool{false,true}{
  rows:=[]*model.Record{{ID:1,Name:"owned"}};probe:=&mergeProbe{expected:rows,fail:failure}
  value,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:handler,DataSource:dml.Source{DB:h.DB},BoundInput:&RecordsInput{Table:"records",Rows:rows,Config:probe}})
  if probe.calls!=1{t.Fatalf("merge calls=%d err=%v",probe.calls,err)}
  if failure{if err==nil{t.Fatal("failed merge reported success")};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{0}})
  }else{if err!=nil{t.Fatal(err)};out,ok:=value.(*RecordsOutput);if !ok||out.Result==nil||out.Result.InsRowsAffected()!=1{t.Fatalf("result=%+v",value)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{ID int;Name string}{{1,"owned"}});if err:=h.ExecStatements(ctx,"DELETE FROM records");err!=nil{t.Fatal(err)}}
 }
}
`
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
	if err := os.WriteFile(filepath.Join(root, "pkg/records/merge_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated Merge owner regression: %v\n%s", err, output)
	}

}
