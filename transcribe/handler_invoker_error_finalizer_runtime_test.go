package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedReaderInvokerErrorFinalizerSQLite(t *testing.T) {
	ctx := t.Context()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/invokererrorfixture", SourceModFile: os.Getenv("DATLY_TEST_MODFILE")}).Write(t, root)
	source := `#package('github.com/viant/datly/invokererrorfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $ID<int>(query/id).Optional().WithPredicate(0,'equal','r','id'))
#define($_ = $Data<[]*Record>(output/view))
SELECT r.id,r.name,type(r,'Record'),CAST(r.id AS int),CAST(r.name AS string) FROM (SELECT id,name FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")} ORDER BY id) r`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/invokererrorfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "get"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	hookPath := filepath.Join(root, "generated", "authored_lifecycle.go")
	if err := os.WriteFile(hookPath, []byte(generatedReaderInvokerErrorHooks), 0600); err != nil {
		t.Fatal(err)
	}
	// A repeat native generation must preserve the authored three-argument hook.
	if _, err := (Generator{Operation: "get"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(hookPath)
	if err != nil || string(after) != generatedReaderInvokerErrorHooks {
		t.Fatalf("authored lifecycle changed: %v", err)
	}
	writeSourceFile(t, root, "generated/invoker_error_test.go", generatedReaderInvokerErrorRuntime)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated reader invoker-error runtime: %v\n%s", err, output)
	}
}

const generatedReaderInvokerErrorHooks = `package generated
import ("context"; xh "github.com/viant/xdatly/handler")
var initCalls, finalCalls int
var finalCause error
var finalProbe func(context.Context,xh.ComponentInvoker,error) error
func (*Input) Init(context.Context,xh.ComponentInvoker) error { initCalls++;return nil }
func (*Output) Finalize(ctx context.Context,invoker xh.ComponentInvoker,cause error) error {
 finalCalls++;finalCause=cause
 if cause!=nil{return nil}
 return finalProbe(ctx,invoker,cause)
}
var _ xh.InvokerInitializer=(*Input)(nil)
var _ xh.InvokerErrorFinalizer=(*Output)(nil)
`

const generatedReaderInvokerErrorRuntime = `package generated
import (
 "context"
 "database/sql"
 "errors"
 "net/http/httptest"
 "reflect"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 rh "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 xh "github.com/viant/xdatly/handler"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func TestNativeReaderLifecycleWithoutInputInvoker(t *testing.T){
 for _,mode:=range []string{"success","child failure","early binding","native read failure"}{t.Run(mode,func(t *testing.T){
  ctx:=t.Context()
  for _,typ:=range []reflect.Type{reflect.TypeFor[Input](),reflect.TypeFor[Output]()} {for i:=0;i<typ.NumField();i++{field:=typ.Field(i);if field.Type.Implements(reflect.TypeFor[xh.ComponentInvoker]()){t.Fatalf("retained invoker field: %s",field.Name)}}}
  db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
  if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT);INSERT INTO records VALUES(1,'one');CREATE TABLE audit(id INTEGER)");err!=nil{t.Fatal(err)}
  holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
  source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
  component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
  resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
  reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
  childSpec:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:"example.test/lifecycle",Name:"Child"},Name:"Child",Routes:[]*spec.Route{{Method:"POST",Path:"/child"}}}
  type ChildOutput struct{}
  childArtifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:childSpec,InputType:reflect.TypeFor[struct{}](),OutputType:reflect.TypeFor[ChildOutput]()});if err!=nil{t.Fatal(err)}
  childCalls,commits:=0,0;failure:=errors.New("child veto")
  child:=&registry.RegisteredComponent{Component:childArtifact.Component,Input:childArtifact.Input,OutputType:reflect.TypeFor[ChildOutput](),DataSource:dml.Source{DB:db,OnCommit:func(context.Context){commits++}},Handler:rh.HandlerFunc(func(ctx context.Context,in rh.Invocation)(any,error){
   childCalls++;value,_,err:=in.Binder.Lookup(ctx,xh.DataKey);if err!=nil{return nil,err}
   if err=value.(xh.Data).Execute("INSERT INTO audit VALUES(1)");err!=nil{return nil,err}
   if mode=="child failure"{return &ChildOutput{},failure};return &ChildOutput{},nil
  })}
  initCalls,finalCalls,finalCause=0,0,nil
  var retained xh.ComponentInvoker
  finalProbe=func(ctx context.Context,invoker xh.ComponentInvoker,cause error)error{
   if cause!=nil{t.Fatalf("successful read boundary cause=%v",cause)}
   retained=invoker
   _,err:=invoker.Invoke(ctx,xh.ComponentRequest{Target:xh.ComponentTarget{Scope:childSpec.Key.Scope,Name:childSpec.Key.Name,Method:"POST",Path:"/child"},Input:&struct{}{}})
   if commits!=0{t.Fatal("child committed before Finalize returned")}
   return err
  }
  rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Reader:reader},child},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
  if mode=="native read failure"{if _,err=db.Exec("DROP TABLE records");err!=nil{t.Fatal(err)}}
  url:="/records";if mode=="early binding"{url+="?id=bad"}
  scope,err:=requestprovider.New(httptest.NewRequest("GET",url,nil));if err!=nil{t.Fatal(err)};defer scope.Close()
  result,err:=rt.ExecuteRoute(ctx,"GET","/records",scope)
  early:=mode=="early binding"||mode=="native read failure"
  if early{if err==nil||result!=nil||finalCalls!=0||childCalls!=0{t.Fatalf("early output=%T err=%v hook=%d child=%d",result,err,finalCalls,childCalls)}}else{
   if finalCalls!=1||childCalls!=1||finalCause!=nil||initCalls!=1{t.Fatalf("native hook init=%d finalize=%d child=%d cause=%v",initCalls,finalCalls,childCalls,finalCause)}
   if mode=="success"{out:=result.(*Output);if err!=nil||len(out.Data)!=1||out.Data[0].Id!=1||commits!=1{t.Fatalf("result=%+v err=%v commits=%d",out,err,commits)}}else if !errors.Is(err,failure)||commits!=0{t.Fatalf("child veto=%v commits=%d",err,commits)}
   if _,err:=retained.Invoke(ctx,xh.ComponentRequest{});err==nil{t.Fatal("invoker survived callback")}
  }
  var count int;if err=db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count);err!=nil{t.Fatal(err)};want:=0;if mode=="success"{want=1};if count!=want{t.Fatalf("audit rows=%d want=%d",count,want)}
 })}
}
`
