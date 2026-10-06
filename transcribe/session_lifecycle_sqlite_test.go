package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGeneratedSessionLifecyclePoliciesSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/sessionfixture"}).Write(t, root)
	source := `#package('github.com/viant/datly/sessionfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.*,type(r,'Record'),root_null_policy(r,'initial-validation'),lifecycle_type(r,'Lifecycle') FROM (SELECT id,name FROM records) r`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/sessionfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/lifecycle.go", sessionPolicyHooks)
	writeSourceFile(t, root, "generated/policy_test.go", sessionPolicyRuntime)
	// Canonical regeneration must retain authored callbacks and stable artifacts.
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string]string {
		result := map[string]string{}
		err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !e.IsDir() {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				result[path] = string(data)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	before := snapshot()
	if before[filepath.Join(root, "generated/lifecycle.go")] != sessionPolicyHooks {
		t.Fatal("authored hook overwritten")
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, snapshot()) {
		t.Fatal("generation is not deterministic")
	}
	command := exec.Command("go", "test", "-race", "-mod=mod", "-count=1", "-timeout=90s", "./generated")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		for path, content := range before {
			if filepath.Base(path) == "input.go" {
				t.Log(content)
			}
		}
		t.Fatalf("generated policy runtime: %v\n%s", err, out)
	}
}

const sessionPolicyHooks = `package generated
import (
 "context"
 "errors"
 rh "github.com/viant/datly/runtime/handler"
 h "github.com/viant/xdatly/handler"
 "github.com/viant/xdatly/logger"
)
type Lifecycle struct { entries int }
func (*Input) Init(ctx context.Context) error {logger.FromContext(ctx).Info("input initialized");return nil}
func (*Lifecycle) Init(context.Context,*Record,h.LifecycleContext[Record,h.NoParent,Output])error{return nil}
func (*Lifecycle) ValidateInput(ctx context.Context,_ *Input,_ *Output,_ h.ValidationReport)error{logger.FromContext(ctx).Info("business validated");return nil}
func (*Lifecycle) ObservePhase(ctx context.Context,e h.PhaseEvent) {
 if e.Boundary!=h.PhaseBegin{return}
 switch e.Phase {case h.PhaseInvocation:logger.FromContext(ctx).Info("execution start");case h.PhaseValidation:logger.FromContext(ctx).Info("validation start");logger.FromContext(ctx).Info("validator run start")}
}
func (p *Lifecycle) ObserveQueueAttempt(ctx context.Context,e h.QueueAttemptEvent) {if e.Boundary==h.PhaseBegin{p.entries++};logger.FromContext(ctx).Info("queue",e,p.entries)}
func (*Lifecycle) AfterQueue(ctx context.Context,_ *Record,_ h.LifecycleContext[Record,h.NoParent,Output])error{logger.FromContext(ctx).Info("after queue");return nil}
func (*Lifecycle) Finalize(ctx context.Context,_ *Input,out *Output,o h.Outcome)error{logger.FromContext(ctx).Info("finalized",o);var structural *h.RootNullRecordError;if errors.As(o.Error,&structural){out.Data=[]*Record{}};return nil}
func (*Lifecycle) Recover(_ context.Context,in *Input,_ *Output,o rh.MutationOutcome)(rh.Recovery,error){if len(in.Records)==1&&in.Records[0]!=nil&&in.Records[0].Id!=nil&&*in.Records[0].Id=="retry"{if o.Attempt==0{return rh.RecoveryRetry,nil};return rh.RecoveryAccept,nil};return rh.RecoveryNone,nil}
`

const sessionPolicyRuntime = `package generated
import (
 "context"
 "database/sql"
 "errors"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 rh "github.com/viant/datly/runtime/handler"
 writer "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 h "github.com/viant/xdatly/handler"
 "github.com/viant/xdatly/logger"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
type receipt struct { messages []string; events []h.QueueAttemptEvent; counts []int; outcomes []h.Outcome }
func(*receipt)Debug(string,...any){}
func(*receipt)Warn(string,...any){}
func(*receipt)Error(string,...any){}
func(p *receipt)Info(msg string,args ...any){if msg=="queue"{p.events=append(p.events,args[0].(h.QueueAttemptEvent));p.counts=append(p.counts,args[1].(int));return};if msg=="finalized"{p.outcomes=append(p.outcomes,args[0].(h.Outcome));return};p.messages=append(p.messages,msg)}
func TestPolicies(t *testing.T){for _,tc:=range []struct{name,body,stored string;null,failure,retry,caller bool;attempts int}{
 {name:"root null",body:"{\"Data\":[null]}",stored:"old",null:true},
 {name:"identity noop",body:"{\"Data\":[{\"id\":\"one\"}]}",stored:"old",attempts:1},
 {name:"same value physical",body:"{\"Data\":[{\"id\":\"one\",\"name\":\"old\"}]}",stored:"old",attempts:1},
 {name:"real update",body:"{\"Data\":[{\"id\":\"one\",\"name\":\"new\"}]}",stored:"new",attempts:1},
 {name:"late flush failure",body:"{\"Data\":[{\"id\":\"one\",\"name\":\"fail\"}]}",stored:"old",failure:true,attempts:1},
 {name:"caller pending",body:"{\"Data\":[{\"id\":\"new\",\"name\":\"new\"}]}",stored:"old",caller:true,attempts:1},
 {name:"native retry",body:"{\"Data\":[{\"id\":\"retry\",\"name\":\"new\"}]}",stored:"old",retry:true,attempts:2},
}{t.Run(tc.name,func(t *testing.T){
 db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT);INSERT INTO records VALUES('one','old');CREATE TRIGGER denied BEFORE UPDATE ON records WHEN NEW.name='fail' BEGIN SELECT RAISE(FAIL,'private failure');END;CREATE TRIGGER ignored BEFORE INSERT ON records WHEN NEW.id='retry' BEGIN SELECT RAISE(IGNORE);END");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)};if component.RootView.RootNullPolicy!="initial-validation"{t.Fatal("root policy lost in generated metadata/bootstrap")}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)};if handler.NewPhaseObserver()==nil{t.Fatal("hook not linked")}
 dataSource:=dml.Source{DB:db};if tc.caller{tx,err:=db.Begin();if err!=nil{t.Fatal(err)};defer tx.Rollback();dataSource.Tx=tx}
 receipt:=&receipt{}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dataSource,Capabilities:rh.InvocationCapabilities{Logger:receipt}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 req:=httptest.NewRequest("PATCH","/records",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)};defer scope.Close()
 ctx:=logger.WithContext(context.Background(),receipt)
 out,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope)
 if tc.null {var structural *h.RootNullRecordError;if !errors.As(err,&structural)||structural.Location!="Records[0]"||len(receipt.events)!=0||len(out.(*Output).Data)!=0{t.Fatalf("root null output=%v err=%v",out,err)};want:=[]string{"execution start","input initialized","validation start","validator run start"};if !reflect.DeepEqual(receipt.messages,want){t.Fatalf("null phases=%v",receipt.messages)}} else if (err!=nil)!=tc.failure{t.Fatal(err)}
 if len(receipt.events)!=tc.attempts*2||len(receipt.outcomes)!=max(1,tc.attempts){t.Fatalf("events=%+v outcomes=%+v",receipt.events,receipt.outcomes)}
 for i,e:=range receipt.events{if e.InvocationID==0||e.Position!=0||e.Location!="Records[0]"||e.Attempt!=i/2{t.Fatal(e)};if i%2==1 {result:=h.QueueQueued;if tc.name=="identity noop"{result=h.QueueNoopCompleted};if e.Result!=result{t.Fatal(e)}}}
 if tc.retry {if receipt.events[0].InvocationID!=receipt.events[2].InvocationID||receipt.counts[0]!=1||receipt.counts[2]!=1{t.Fatal("retry reused state or identity")}}
 if tc.caller {if receipt.outcomes[0].Transactions[0].State!=h.TransactionCallerPending{t.Fatal(receipt.outcomes)};if err=dataSource.Tx.Rollback();err!=nil{t.Fatal(err)}}
 if tc.failure {if receipt.outcomes[0].Transactions[0].State!=h.TransactionRolledBack{t.Fatal(receipt.outcomes)}}
 // Current reads participate in the native invocation transaction even when the row queues no DML.
 if tc.name=="identity noop" {if len(receipt.outcomes[0].Transactions)!=1 || receipt.outcomes[0].Transactions[0].State!=h.TransactionCommitted || receipt.outcomes[0].Transactions[0].Error!=nil {t.Fatalf("noop transaction evidence: %+v",receipt.outcomes[0])};for _,msg:=range receipt.messages{if msg=="after queue"{t.Fatal("noop dispatched AfterQueue")}}}
 var stored string;if err=db.QueryRow("SELECT name FROM records WHERE id='one'").Scan(&stored);err!=nil||stored!=tc.stored{t.Fatal(stored,err)}
 var count int;if err=db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count);err!=nil||count!=1{t.Fatal(count,err)}
 var tables int;if err=db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables);err!=nil||tables!=1{t.Fatal("unexpected product/allocator table",tables,err)}
})}}
`
