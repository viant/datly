package transcribe

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestSequenceStrategyGeneratedRuntimeSQLite(t *testing.T) {
	for _, strategy := range []string{"", "transient", "reservation"} {
		t.Run("strategy="+strategy, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/seqstrategyfixture"}).Write(t, root)
			setting := ""
			if strategy != "" {
				setting = fmt.Sprintf("#setting($_ = $sequence_strategy(%q))\n", strategy)
			}
			text := setting + `#setting($_ = $route('/records','POST'))
#define($_ = $Records<[]*RecordsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.* FROM RECORDS r`
			source := &Source{Scope: "github.com/viant/datly/seqstrategyfixture/generated", Name: "Records", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": h.DB})}
			generated, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost}}})
			if err != nil {
				t.Fatal(err)
			}
			if generated.Result.Plan.Settings.SequenceStrategy != strategy {
				t.Fatal("strategy absent from generation plan")
			}
			writeSourceFile(t, root, "generated/sequence_runtime_test.go", strings.ReplaceAll(sequenceStrategyFixture, "{{STRATEGY}}", strategy))
			cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("generated strategy runtime: %v\n%s", err, out)
			}
		})
	}
}

const sequenceStrategyFixture = `package records
import(
 "context"
 "fmt"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 custom "github.com/viant/datly/runtime/handler/custom"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/sqlx/metadata/info/dialect"
)
type observedSource struct{dml.Source;called *string}
func(s observedSource)WithSequenceStrategy(value string)(dexec.DataSource,error){*s.called=value;return s.Source.WithSequenceStrategy(value)}
func TestGeneratedStrategyRuntime(t *testing.T){
 for _,external:=range []bool{false,true}{t.Run(fmt.Sprint(external),func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t);h.DB.SetMaxOpenConns(1)
  if err:=h.ExecStatements(ctx,"CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT NOT NULL)");err!=nil{t.Fatal(err)}
  tagged,ok,err:=dtag.ParseComponent(reflect.TypeOf(Component{}).Field(0).Tag)
  if err!=nil||!ok{t.Fatalf("generated holder tag: %v %v",ok,err)}
  route:=&bootstrap.RouteSource{HolderType:"Component",FieldName:"Contract",PackageName:"records",PackagePath:"github.com/viant/datly/seqstrategyfixture/generated",InputType:"RecordsInput",OutputType:"RecordsOutput",Tag:tagged}
  component,err:=route.Resolve(reflect.TypeOf(RecordsInput{}),reflect.TypeOf(RecordsOutput{}));if err!=nil{t.Fatal(err)}
  if component.Settings==nil||component.Settings.SequenceStrategy!="{{STRATEGY}}"{t.Fatal("bootstrap lost strategy")}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{})});if err!=nil{t.Fatal(err)}
  source:=dml.Source{DB:h.DB};called:=""
  if "{{STRATEGY}}"!=""{source.SequenceStrategy=dialect.PresetIDWithMax} // would fail if canonical DQL policy is not applied
  if external{tx,err:=h.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer tx.Rollback();source.Tx=tx}
  handler:=custom.New[RecordsInput,RecordsOutput](NewRecordsHandler())
  rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(RecordsOutput{}),Handler:handler,DataSource:observedSource{source,&called}}});if err!=nil{t.Fatal(err)}
  request:=httptest.NewRequest("POST","/records",strings.NewReader("{\"Data\":[{\"name\":\"selected\"}]}"));request.Header.Set("Content-Type","application/json")
  scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
  if _,err=rt.ExecuteRoute(ctx,"POST","/records",scope);err!=nil{t.Fatal(err)}
  if called!="{{STRATEGY}}"{t.Fatalf("runtime did not apply canonical setting: %q",called)}
  var id int64
  if external{if err=source.Tx.QueryRowContext(ctx,"SELECT ID FROM RECORDS").Scan(&id);err!=nil{t.Fatal("caller transaction not retained:",err)};if err=source.Tx.Rollback();err!=nil{t.Fatal(err)}
  }else{if err=h.DB.QueryRowContext(ctx,"SELECT ID FROM RECORDS").Scan(&id);err!=nil{t.Fatal(err)}}
  if id!=1{t.Fatalf("native allocation not executed: %d",id)}
  var count int;h.DB.QueryRowContext(ctx,"SELECT count(*) FROM RECORDS").Scan(&count)
  want:=1;if external{want=0};if count!=want{t.Fatalf("transaction outcome count=%d want=%d",count,want)}
 })}
}
`
