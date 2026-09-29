package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os/exec"
	"strings"
	"testing"
)

func TestGeneratedScopedSequenceSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT,UNIQUE(turn_id,sequence))"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/scopedfixture"}).Write(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "messages", Scope: "github.com/viant/datly/scopedfixture/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB}), Text: scopedSequenceDQL}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{"sequence_scope(r.sequence)", "sequence_scope(r.sequence,other.turn_id)", "sequence_scope(r.sequence,r.turn_id,r.turn_id)", "sequence_scope(r.sequence,r.missing)"} {
		invalid := request
		copy := *request.Source
		invalid.Source = &copy
		invalid.Destination = t.TempDir()
		invalid.Source.Text = strings.Replace(scopedSequenceDQL, "sequence_scope(r.sequence,r.turn_id)", bad, 1)
		if _, e := (Generator{Operation: "patch"}).Generate(ctx, invalid); e == nil {
			t.Fatalf("invalid annotation accepted: %s", bad)
		}
	}
	invalid := request
	copy := *request.Source
	invalid.Source = &copy
	invalid.Destination = t.TempDir()
	invalid.Source.Text = strings.Replace(scopedSequenceDQL, "'/messages','PATCH'", "'/messages','GET'", 1)
	if _, e := (Generator{Operation: "get"}).Generate(ctx, invalid); e == nil {
		t.Fatal("reader accepted sequence_scope")
	}
	writeSourceFile(t, root, "generated/scoped_sequence_test.go", scopedSequenceRuntime)
	writeSourceFile(t, root, "generated/scoped_sequence_live_test.go", scopedSequenceLiveRuntime)
	writeSourceFile(t, root, "generated/lifecycle.go", scopedSequenceHooks)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated sequence runtime: %v\n%s", err, out)
	}
	// The declaration must regenerate identically rather than live in edited DTOs.
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
}

// The opt-in policy is exercised through generated metadata and actual storage.
func TestGeneratedScopedSequenceNullAllocationSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT,UNIQUE(turn_id,sequence))"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/scopedfixture"}).Write(t, root)
	source := strings.Replace(scopedSequenceDQL, "sequence_scope(r.sequence,r.turn_id)", `sequence_scope(r.sequence,r.turn_id),tag(r.sequence,'sequenceOnNull:"allocate"')`, 1)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "messages", Scope: "github.com/viant/datly/scopedfixture/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB}), Text: source}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	runtime := strings.Replace(scopedSequenceRuntime, `"explicit null preserved"`, `"explicit null allocates"`, 1)
	runtime = strings.Replace(runtime, `map[string]*int{"null":nil}`, `map[string]*int{"null":pointer(3)}`, 1)
	writeSourceFile(t, root, "generated/scoped_sequence_test.go", runtime)
	writeSourceFile(t, root, "generated/lifecycle.go", scopedSequenceHooks)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated null policy: %v\n%s", err, out)
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
}

const scopedSequenceDQL = `#package('github.com/viant/datly/scopedfixture/generated')
#setting($_ = $route('/messages','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Messages<[]*Message>(body/Data))
#define($_ = $Data<[]*Message>(output/body))
SELECT r.id,r.turn_id,r.sequence,r.title,type(r,'Message'),lifecycle_type(r,'Lifecycle'),
 CAST(r.id AS string),tag(r.id,'sqlx:"id,primaryKey" validate:"required"'),
 CAST(r.turn_id AS *string),CAST(r.sequence AS *int),sequence_scope(r.sequence,r.turn_id)
FROM messages r`
const scopedSequenceRuntime = `package generated
import(
 "context"
 "database/sql"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/runtime/registry"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/mattn/go-sqlite3"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func execute(t *testing.T,db *sql.DB,tx *sql.Tx,body string)(*Output,error){t.Helper()
 holder:=reflect.TypeFor[MessagesComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatalf("holder: %v",err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(MessagesDatlyResourceNamespace,MessagesDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db,Tx:tx}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 request:=httptest.NewRequest("PATCH","/messages",strings.NewReader(body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 value,err:=rt.ExecuteRoute(context.Background(),"PATCH","/messages",scope);if err!=nil{return nil,err};return value.(*Output),nil
}
func TestScopedWriter(t *testing.T){
 type useCase struct{desc,body string;expect map[string]*int;failed bool}
 pointer:=func(v int)*int{return &v}
 for _,tc:=range []useCase{
  {"two partitions",` + "`" + `{"Data":[{"id":"new1","turnId":"t1"},{"id":"new2","turnId":"t2"}]}` + "`" + `,map[string]*int{"new1":pointer(3),"new2":pointer(101)},false},
  {"batch reserves supplied values",` + "`" + `{"Data":[{"id":"explicit","turnId":"t1","sequence":3},{"id":"new1","turnId":"t1"},{"id":"new2","turnId":"t1"}]}` + "`" + `,map[string]*int{"explicit":pointer(3),"new1":pointer(4),"new2":pointer(5)},false},
  {"explicit zero preserved",` + "`" + `{"Data":[{"id":"zero","turnId":"t1","sequence":0}]}` + "`" + `,map[string]*int{"zero":pointer(0)},false},
  {"explicit null preserved",` + "`" + `{"Data":[{"id":"null","turnId":"t1","sequence":null}]}` + "`" + `,map[string]*int{"null":nil},false},
  {"empty turn is unsequenced",` + "`" + `{"Data":[{"id":"empty","turnId":""}]}` + "`" + `,map[string]*int{"empty":nil},false},
  {"update null clears sequence",` + "`" + `{"Data":[{"id":"old1","sequence":null}]}` + "`" + `,map[string]*int{"old1":nil},false},
  {"update does not resequence",` + "`" + `{"Data":[{"id":"old1","title":"changed"}]}` + "`" + `,map[string]*int{"old1":pointer(2)},false},
  {"supplied conflict is not repaired",` + "`" + `{"Data":[{"id":"collision","turnId":"t1","sequence":2}]}` + "`" + `,nil,true},
 }{t.Run(tc.desc,func(t *testing.T){db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
  _,err=db.Exec("CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT,UNIQUE(turn_id,sequence));INSERT INTO messages VALUES('old1','t1',2,'old'),('old2','t2',100,'old')");if err!=nil{t.Fatal(err)}
  _,err=execute(t,db,nil,tc.body);if (err!=nil)!=tc.failed{t.Fatalf("error=%v expected failure=%v",err,tc.failed)}
  for id,want:=range tc.expect{var value sql.NullInt64;if err=db.QueryRow("SELECT sequence FROM messages WHERE id=?",id).Scan(&value);err!=nil{t.Fatal(err)};if want==nil{if value.Valid{t.Fatalf("%s unexpectedly sequenced",id)}}else if !value.Valid||int(value.Int64)!=*want{t.Fatalf("%s sequence=%v expected=%d",id,value,*want)}}
 })}
}
func TestScopedWriterCallerTransaction(t *testing.T){
 for _,commit:=range []bool{false,true}{t.Run(map[bool]string{false:"rollback",true:"commit"}[commit],func(t *testing.T){db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
  if _,err=db.Exec("CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT,UNIQUE(turn_id,sequence))");err!=nil{t.Fatal(err)}
  tx,err:=db.BeginTx(context.Background(),nil);if err!=nil{t.Fatal(err)};defer tx.Rollback()
  if _,err=execute(t,db,tx,` + "`" + `{"Data":[{"id":"new","turnId":"t1"}]}` + "`" + `);err!=nil{t.Fatal(err)}
  var n int;if err=tx.QueryRow("SELECT sequence FROM messages WHERE id='new'").Scan(&n);err!=nil||n!=1{t.Fatalf("pending sequence=%d error=%v",n,err)}
  if commit{err=tx.Commit()}else{err=tx.Rollback()};if err!=nil{t.Fatal(err)}
  if err=db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&n);err!=nil{t.Fatal(err)};want:=0;if commit{want=1};if n!=want{t.Fatalf("rows=%d expected=%d",n,want)}
 })}
}
`

const scopedSequenceLiveRuntime = `package generated
import("context";"database/sql";"fmt";"os";"path/filepath";"sync";"testing";"github.com/viant/sqlx/io/sequence";_ "github.com/go-sql-driver/mysql";_ "github.com/viant/sqlx/metadata/product/mysql")
func TestScopedWriterIndependentConnections(t *testing.T){
 dsn:=filepath.Join(t.TempDir(),"race.db")+"?_busy_timeout=10000";db,err:=sql.Open("sqlite3",dsn);if err!=nil{t.Fatal(err)};defer db.Close()
 if _,err=db.Exec("CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT,UNIQUE(turn_id,sequence))");err!=nil{t.Fatal(err)}
 const count=6;var wg sync.WaitGroup;failures:=make(chan error,count)
 for i:=0;i<count;i++{wg.Add(1);go func(i int){defer wg.Done();connection,e:=sql.Open("sqlite3",dsn);if e!=nil{failures<-e;return};defer connection.Close();connection.SetMaxOpenConns(1);_,e=execute(t,connection,nil,fmt.Sprintf("{\"Data\":[{\"id\":\"m%d\",\"turnId\":\"t1\"}]}",i));if e!=nil{failures<-e}}(i)}
 wg.Wait();close(failures);for e:=range failures{t.Error(e)}
 var rows,distinct int;if err=db.QueryRow("SELECT COUNT(*),COUNT(DISTINCT sequence) FROM messages").Scan(&rows,&distinct);err!=nil{t.Fatal(err)};if rows!=count||distinct!=count{t.Fatalf("rows=%d distinct=%d",rows,distinct)}
}
func TestScopedWriterMySQLLive(t *testing.T){
 dsn:=os.Getenv("SQLX_SCOPED_MYSQL_DSN");if dsn==""{t.Skip("live MySQL fixture not configured")}
 db,err:=sql.Open("mysql",dsn);if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if err=sequence.Provision(context.Background(),db,"mysql");err!=nil{t.Fatal(err)}
 if _,err=db.Exec("DELETE FROM sqlx_scoped_sequences");err!=nil{t.Fatal(err)}
 defer db.Exec("DROP TABLE IF EXISTS messages")
 if _,err=db.Exec("CREATE TABLE messages(id VARCHAR(64) PRIMARY KEY,turn_id VARCHAR(64),sequence BIGINT,title TEXT,UNIQUE(turn_id,sequence)) ENGINE=InnoDB");err!=nil{t.Fatal(err)}
 if _,err=db.Exec("INSERT INTO messages VALUES('old1','t1',2,'old'),('old2','t2',100,'old')");err!=nil{t.Fatal(err)}
 for _,commit:=range []bool{false,true}{t.Run(fmt.Sprint(commit),func(t *testing.T){tx,e:=db.BeginTx(context.Background(),nil);if e!=nil{t.Fatal(e)};defer tx.Rollback()
  if _,e=execute(t,db,tx,"{\"Data\":[{\"id\":\"new1\",\"turnId\":\"t1\"},{\"id\":\"new2\",\"turnId\":\"t2\"}]}");e!=nil{t.Fatal(e)}
  var first,second int;if e=tx.QueryRow("SELECT sequence FROM messages WHERE id='new1'").Scan(&first);e!=nil{t.Fatal(e)};if e=tx.QueryRow("SELECT sequence FROM messages WHERE id='new2'").Scan(&second);e!=nil{t.Fatal(e)};if first!=3||second!=101{t.Fatalf("sequences=%d,%d",first,second)}
  if commit{e=tx.Commit()}else{e=tx.Rollback()};if e!=nil{t.Fatal(e)}
 })}
}
func TestScopedWriterRetriesOnlyAutomaticCollisions(t *testing.T){
 dsn:=os.Getenv("SQLX_SCOPED_MYSQL_DSN");if dsn==""{t.Skip("live MySQL fixture not configured")}
 db,err:=sql.Open("mysql",dsn);if err!=nil{t.Fatal(err)};defer db.Close()
 if err=sequence.Provision(context.Background(),db,"mysql");err!=nil{t.Fatal(err)}
 if _,err=db.Exec("DELETE FROM sqlx_scoped_sequences");err!=nil{t.Fatal(err)}
 defer db.Exec("DROP TABLE IF EXISTS messages")
 if _,err=db.Exec("CREATE TABLE messages(id VARCHAR(64) PRIMARY KEY,turn_id VARCHAR(64),sequence BIGINT,title TEXT,UNIQUE(turn_id,sequence)) ENGINE=InnoDB");err!=nil{t.Fatal(err)}
 attempts:=0
 raceHook=func(entity *Message){if entity.Id!="wanted"{return};attempts++;if attempts==1 {other,e:=sql.Open("mysql",dsn);if e!=nil{t.Fatal(e)};defer other.Close();if _,e=other.Exec("INSERT INTO messages VALUES('competitor','race',?,'winner')",*entity.Sequence);e!=nil{t.Fatal(e)}}}
 defer func(){raceHook=nil}()
 if _,err=execute(t,db,nil,"{\"Data\":[{\"id\":\"wanted\",\"turnId\":\"race\"}]}");err!=nil{t.Fatal(err)}
 var allocated int;if err=db.QueryRow("SELECT sequence FROM messages WHERE id='wanted'").Scan(&allocated);err!=nil{t.Fatal(err)}
 if attempts!=2||allocated!=2 {t.Fatalf("attempts=%d sequence=%d",attempts,allocated)}
 attempts=0
 raceHook=func(entity *Message){if entity.Id=="explicit"{attempts++}}
 if _,err=execute(t,db,nil,"{\"Data\":[{\"id\":\"explicit\",\"turnId\":\"race\",\"sequence\":1}]}");err==nil{t.Fatal("explicit conflict was repaired")}
 if attempts!=1{t.Fatalf("explicit sequence replayed %d times",attempts)}
}
func TestScopedWriterRetrySafety(t *testing.T){
 dsn:=os.Getenv("SQLX_SCOPED_MYSQL_DSN");if dsn==""{t.Skip("live MySQL fixture not configured")}
 type useCase struct{desc,kind string;expectedAttempts int}
 for _,tc:=range []useCase{{"bounded exhaustion","exhaust",10},{"concurrent primary key is never overwritten","identity",1},{"caller transaction is never replayed","caller",1}}{t.Run(tc.desc,func(t *testing.T){
  db,e:=sql.Open("mysql",dsn);if e!=nil{t.Fatal(e)};defer db.Close()
  if e=sequence.Provision(context.Background(),db,"mysql");e!=nil{t.Fatal(e)}
  if _,e=db.Exec("DELETE FROM sqlx_scoped_sequences");e!=nil{t.Fatal(e)}
  defer db.Exec("DROP TABLE IF EXISTS messages")
  if _,e=db.Exec("CREATE TABLE messages(id VARCHAR(64) PRIMARY KEY,turn_id VARCHAR(64),sequence BIGINT,title TEXT,UNIQUE(turn_id,sequence)) ENGINE=InnoDB");e!=nil{t.Fatal(e)}
  attempts:=0
  raceHook=func(entity *Message){if entity.Id!="wanted"{return};attempts++;other,err:=sql.Open("mysql",dsn);if err!=nil{t.Fatal(err)};defer other.Close();id:=fmt.Sprintf("competitor-%d",attempts);if tc.kind=="identity"{id="wanted"};if _,err=other.Exec("INSERT INTO messages VALUES(?,'race',?,'winner')",id,*entity.Sequence);err!=nil{t.Fatal(err)}}
  defer func(){raceHook=nil}()
  var tx *sql.Tx
  if tc.kind=="caller"{tx,e=db.BeginTx(context.Background(),nil);if e!=nil{t.Fatal(e)};defer tx.Rollback()}
  _,e=execute(t,db,tx,"{\"Data\":[{\"id\":\"wanted\",\"turnId\":\"race\"}]}")
  if e==nil{t.Fatal("collision unexpectedly succeeded")}
  if attempts!=tc.expectedAttempts{t.Fatalf("attempts=%d expected=%d error=%v",attempts,tc.expectedAttempts,e)}
  if tx!=nil{if e=tx.Rollback();e!=nil{t.Fatal(e)}}
  var count int;if e=db.QueryRow("SELECT COUNT(*) FROM messages WHERE id='wanted' AND title='winner'").Scan(&count);e!=nil{t.Fatal(e)}
  expected:=0;if tc.kind=="identity"{expected=1};if count!=expected{t.Fatalf("winner rows=%d expected=%d",count,expected)}
 })}
}

`

const scopedSequenceHooks = `package generated
import("context";"reflect";xhandler "github.com/viant/xdatly/handler")
type Lifecycle struct{}
func LifecycleDatlyType()reflect.Type{return reflect.TypeFor[Lifecycle]()}
var LifecycleDatly=LifecycleDatlyType()
var raceHook func(*Message)
func(*Lifecycle)Init(context.Context,*Message,xhandler.LifecycleContext[Message,xhandler.NoParent,Output])error{return nil}
func(*Lifecycle)Validate(context.Context,*Message,xhandler.LifecycleContext[Message,xhandler.NoParent,Output])error{return nil}
func(*Lifecycle)AfterQueue(context.Context,*Message,xhandler.LifecycleContext[Message,xhandler.NoParent,Output])error{return nil}
func(*Lifecycle)Finalize(context.Context,*Input,*Output,xhandler.Outcome)error{return nil}

func(*Lifecycle)AfterSequence(_ context.Context,entity *Message,_ xhandler.LifecycleContext[Message,xhandler.NoParent,Output])error {if raceHook!=nil {raceHook(entity)};return nil}
`
