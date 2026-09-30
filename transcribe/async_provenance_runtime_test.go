package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGeneratedAsyncMutationRefreshesProvenanceSQLite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, QTY INTEGER NOT NULL DEFAULT 0, NOTE TEXT)`); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/asyncgenerated"}).Write(t, root)
	dql := "#setting($_ = $route('/events', 'PATCH'))\n" +
		"#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())\n" +
		"#define($_ = $MatchKey<string>(query/key))\n" +
		"#define($_ = $JobStatus<string>(async/jobinfo.status).Output())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /*\n? SELECT ARRAY_AGG(Id) AS IDs FROM `/` LIMIT 1\n*/)\n" +
		`#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
 SELECT ID, NAME, QTY, NOTE FROM EVENTS
 WHERE ID IN (#foreach($id in $CurEventsId.IDs)$id#if($foreach.HasNext),#end#end)
 */)
 #define($_ = $Data<[]*EventsView>(output/body))
 SELECT ID, NAME, QTY, NOTE FROM EVENTS`
	_, err := NewCompiler().Transcribe(ctx, Request{Source: &Source{Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql, Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB})}, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Output: "Data", Current: "CurrentEvents", Go: GoHandlerOptions{Execution: GoExecutionMutation}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "async_provenance_test.go"), []byte(generatedAsyncProvenanceSource), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=90s", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated async provenance: %v\n%s", err, output)
	}
}

const generatedAsyncProvenanceSource = `package events

import (
 "context"
 "errors"
 "fmt"
 "reflect"
 "strings"
 "testing"
 "time"
 "net/http/httptest"
 "path/filepath"
 "os"
 "github.com/viant/datly/application"
 gateway "github.com/viant/datly/gateway/http"
 jobstorage "github.com/viant/datly/gateway/async"
 "github.com/viant/datly/typecatalog"
 "github.com/viant/bindly/locator"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 writerhandler "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/runtime/jobs"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 sqldml "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 xasync "github.com/viant/xdatly/async"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)

func(i *EventsInput) Init(context.Context)error{
 if len(i.Events)>0&&i.Events[0].Id!=nil&&*i.Events[0].Id==-1{panic("generated input panic")}
 return nil
}
func TestAsyncGeneratedMutation(t *testing.T){
 for _,mode:=range []string{"commit","rollback","denied","panic","unknown","HTTP"}{t.Run(mode,func(t *testing.T){
  ctx:=context.Background();app:=testharness.NewSQLiteHarness(t);app.DB.SetMaxOpenConns(1)
  if err:=app.ExecStatements(ctx,"PRAGMA foreign_keys=ON","CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, QTY INTEGER NOT NULL DEFAULT 0, NOTE TEXT)","INSERT INTO EVENTS(ID,NAME,QTY,NOTE) VALUES(1,'before',9,'old-note'),(2,'before-two',5,'old-two'),(3,'before-three',7,'third-note')");err!=nil{t.Fatal(err)}
  storeDB:=testharness.NewSQLiteHarness(t);if err:=storeDB.ExecStatements(ctx,sqlite.DatlyJobsSchema);err!=nil{t.Fatal(err)}
  component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Name:"Events",Scope:"example.com/generated/events"},Settings:&spec.Settings{Mutation:"patch"},RootView:&spec.View{Name:"Events",Source:&spec.ViewSource{Table:"EVENTS"},Columns:[]*spec.Column{{Name:"ID",Source:"ID",Type:spec.TypeRef{Name:"int64"},PrimaryKey:true,AutoIncrement:true},{Name:"NAME",Source:"NAME",Type:spec.TypeRef{Name:"string"}},{Name:"QTY",Source:"QTY",Type:spec.TypeRef{Name:"int64"}},{Name:"NOTE",Source:"NOTE",Type:spec.TypeRef{Name:"string"}}}},Routes:[]*spec.Route{{Method:"PATCH",Path:"/events"}},Parameters:[]*spec.Parameter{
   {Name:"Events",Source:spec.BindSource{Kind:"body",Name:"Data"}},
   {Name:"MatchKey",Source:spec.BindSource{Kind:"query",Name:"key"}},
   {Name:"JobStatus",Source:spec.BindSource{Kind:"async",Name:"jobinfo.status"},EmitOutput:true},
   {Name:"CurEventsId",Source:spec.BindSource{Kind:"param",Name:"Events"},Codec:&spec.Codec{Body:"structql",Args:[]string{"SELECT ARRAY_AGG(Id) AS IDs FROM ` + "`" + `/` + "`" + ` LIMIT 1"}}},
   {Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"CurrentEvents"},Cardinality:"many"},
   {Name:"Data",Source:spec.BindSource{Kind:"output",Name:"body"},EmitOutput:true},
  },Views:[]*spec.View{{Key:spec.Key{Kind:spec.KindView,Name:"CurrentEvents",Scope:"example.com/generated/events"},Name:"CurrentEvents",Source:&spec.ViewSource{SQL:"SELECT ID, NAME, QTY, NOTE FROM EVENTS WHERE ID IN (#foreach($id in $CurEventsId.IDs)$id#if($foreach.HasNext),#end#end)"},Columns:[]*spec.Column{
   {Name:"ID",Source:"ID",Type:spec.TypeRef{Name:"int64"},PrimaryKey:true},{Name:"NAME",Source:"NAME",Type:spec.TypeRef{Name:"string"}},{Name:"QTY",Source:"QTY",Type:spec.TypeRef{Name:"int64"}},{Name:"NOTE",Source:"NOTE",Type:spec.TypeRef{Name:"string"}},
  }}}}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(EventsInput{}),OutputType:reflect.TypeOf(EventsOutput{})});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:app.DB}});if err!=nil{t.Fatal(err)}
  handler,err:=writerhandler.New(artifact.Component,reflect.TypeOf(EventsInput{}),reflect.TypeOf(EventsOutput{}),"patch");if err!=nil{t.Fatal(err)}
  components:=[]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(EventsOutput{}),Handler:handler,Providers:[]locator.Provider{views},DataSource:sqldml.Source{DB:app.DB}}}
  rt,err:=druntime.NewRuntime(components);if err!=nil{t.Fatal(err)}
  store,err:=(bootstrap.JobStoreConfig{SQL:&dsql.SQLComponent{DB:storeDB.DB}}).NewStore(ctx);if err!=nil{t.Fatal(err)}
  notifications:=0
  replayReady,replayRelease:=make(chan struct{}),make(chan struct{})
  authorize:=jobs.Authorizer(func(invokeCtx context.Context,access jobs.Access)error{
   if access.Action==jobs.Replay&&mode=="HTTP"{close(replayReady);select{case <-replayRelease:case <-invokeCtx.Done():return invokeCtx.Err()}}
   if access.Action==jobs.Replay&&mode=="denied"{return errors.New("denied")}
   if access.Input!=nil{
    input:=access.Input.(*EventsInput)
    if len(input.CurrentEvents)!=0{t.Fatal("stale current reached authorization")}
    if len(input.Events)<3||input.Events[0].Has==nil||!input.Events[0].Has.Qty||input.Events[2].Has.Qty{t.Fatal("absent versus zero presence was lost")}
   }
   return nil
  })
  service,err:=rt.NewAsyncService(jobs.Config{Store:store,Authorize:authorize,Notify:func(context.Context,*xasync.Job)error{notifications++;return nil}});if err!=nil{t.Fatal(err)}
  var manager *application.Manager
  if mode=="HTTP"{
   sourceDir:=filepath.Join(t.TempDir(),"jobs");if err:=os.MkdirAll(sourceDir,0700);err!=nil{t.Fatal(err)}
   manager,err=application.New(nil,application.WithAsync(application.AsyncConfig{Store:store,Authorize:authorize,Notification:xasync.Notification{Method:xasync.NotificationMethodStorage,Destination:sourceDir},Watch:jobstorage.WatchConfig{JobURL:sourceDir,FailedJobURL:sourceDir+"-failed",PollInterval:5*time.Millisecond}}));if err!=nil{t.Fatal(err)}
   defer manager.Shutdown(context.Background())
   defer func(){select{case <-replayRelease:default:close(replayRelease)}}()
   if err=manager.Reload(ctx,application.Request{Revision:1,Compile:func(context.Context,*typecatalog.Catalog)(*application.Build,error){return &application.Build{Components:components,HTTP:gateway.Config{Async:[]gateway.AsyncRoute{{Route:spec.RouteRef{Method:"PATCH",Path:"/events"},MatchKey:"MatchKey"}}}},nil}});err!=nil{t.Fatal(err)}
  }
  source:="{\"Events\":[{\"id\":1,\"name\":\"after\",\"qty\":0},{\"id\":2,\"name\":\"recreated\",\"qty\":4},{\"id\":3,\"name\":\"third-after\"},{\"name\":\"new\",\"qty\":5}],\"CurrentEvents\":[{\"id\":2,\"name\":\"stale\"}],\"CurEventsId\":{\"IDs\":[999]}}"
  if mode=="panic"{source=strings.Replace(source,"\"id\":1","\"id\":-1",1)}
  var record *jobs.Record
  if mode=="HTTP"{
   request:=httptest.NewRequest("PATCH","/events?key=generated",strings.NewReader(strings.Replace(source,"\"Events\"","\"Data\"",1)))
   request.Header.Set("Content-Type","application/json")
   response:=httptest.NewRecorder();manager.ServeHTTP(response,request)
   if response.Code!=200{t.Fatalf("HTTP capture: %d %s",response.Code,response.Body.String())}
   select{case <-replayReady:case <-time.After(5*time.Second):t.Fatal("watcher did not prepare replay")}
   var id string;if err:=storeDB.DB.QueryRow("SELECT ID FROM DATLY_JOBS").Scan(&id);err!=nil{t.Fatal(err)}
   record,err=store.Get(ctx,id)
   var name string;if err:=app.DB.QueryRow("SELECT NAME FROM EVENTS WHERE ID=1").Scan(&name);err!=nil||name!="before"{t.Fatalf("HTTP capture executed mutation: %s %v",name,err)}
  }else{
   scheduled,err:=service.Schedule(ctx,jobs.Submission{Job:xasync.Job{Request:xasync.Request{Method:"PATCH",URI:"/events"}},SourceState:source});if err!=nil{t.Fatal(err)}
   record,err=store.Get(ctx,scheduled.Job.ID)
  }
  if err!=nil{t.Fatal(err)}
  if strings.Contains(record.State,"CurrentEvents")||strings.Contains(record.State,"CurEventsId"){t.Fatalf("internal values persisted: %s",record.State)}
  if err:=app.ExecStatements(ctx,"UPDATE EVENTS SET QTY=8,NOTE='fresh-note' WHERE ID=1","DELETE FROM EVENTS WHERE ID=2","UPDATE EVENTS SET QTY=11 WHERE ID=3");err!=nil{t.Fatal(err)}
  if mode=="rollback"{if err:=app.ExecStatements(ctx,"CREATE TRIGGER fail_insert BEFORE INSERT ON EVENTS BEGIN SELECT RAISE(ABORT,'rollback proof'); END");err!=nil{t.Fatal(err)}}
  if mode=="unknown"{if err:=app.ExecStatements(ctx,"CREATE TABLE PARENT(ID INTEGER PRIMARY KEY)","CREATE TABLE CHILD(ID INTEGER REFERENCES PARENT(ID) DEFERRABLE INITIALLY DEFERRED)","CREATE TRIGGER unknown_commit AFTER INSERT ON EVENTS BEGIN INSERT INTO CHILD VALUES(999); END");err!=nil{t.Fatal(err)}}
  var result any;var runErr error
  if mode=="HTTP"{
   close(replayRelease)
   deadline:=time.Now().Add(5*time.Second)
   for{record,err=store.Get(ctx,record.ID);if err!=nil{t.Fatal(err)};if record.Status==xasync.StatusDone||record.Status==xasync.StatusError{break};if time.Now().After(deadline){t.Fatalf("watcher did not complete: %+v",record)};time.Sleep(5*time.Millisecond)}
   if record.Status!=xasync.StatusDone{t.Fatalf("HTTP generated replay: %+v",record.Error)}
  }else{result,runErr=service.Run(ctx,record.ID)}
  record,err=store.Get(ctx,record.ID);if err!=nil{t.Fatal(err)}
  want:=xasync.StatusDone
  if mode=="denied"{want=xasync.StatusPending}else if mode=="unknown"{want=xasync.StatusRunning}else if mode!="commit"&&mode!="HTTP"{want=xasync.StatusError}
  if record.Status!=want{t.Fatalf("mode=%s status=%s error=%v",mode,record.Status,runErr)}
  if mode=="commit"||mode=="HTTP"{
   if runErr!=nil{t.Fatal(runErr)}
   if mode!="HTTP"{output:=result.(*EventsOutput);if len(output.Data)!=4||output.Data[3].Id==nil||*output.Data[3].Id<=3{t.Fatalf("stable IDs missing: %+v",output.Data)}}
   var qty int;var note string;if err:=app.DB.QueryRow("SELECT QTY,NOTE FROM EVENTS WHERE ID=1").Scan(&qty,&note);err!=nil||qty!=0||note!="fresh-note"{t.Fatalf("fresh previous/zero value: qty=%d note=%s err=%v",qty,note,err)}
   var freshQty int;if err:=app.DB.QueryRow("SELECT QTY FROM EVENTS WHERE ID=3").Scan(&freshQty);err!=nil||freshQty!=11{t.Fatalf("absent quantity lost fresh backfill: %d %v",freshQty,err)}
   var recreated int;if err:=app.DB.QueryRow("SELECT count(*) FROM EVENTS WHERE ID=2 AND NAME='recreated'").Scan(&recreated);err!=nil||recreated!=1{t.Fatalf("fresh absence did not select INSERT: %d %v",recreated,err)}
  }else if runErr==nil{t.Fatal("expected failure")}
  if mode=="unknown"&&!errors.Is(runErr,jobs.ErrCompletionPending){t.Fatalf("unknown completion=%v",runErr)}
  if mode=="denied"||mode=="unknown"{if notifications!=0||record.EndTime!=nil{t.Fatal("unconfirmed completion notified")}}
  if mode=="rollback"||mode=="denied"||mode=="panic"{var name string;if err:=app.DB.QueryRow("SELECT NAME FROM EVENTS WHERE ID=1").Scan(&name);err!=nil||name!="before"{t.Fatalf("failed mutation wrote %s: %v",name,err)}}
  _ = fmt.Sprint(result)
 })}
}
`
