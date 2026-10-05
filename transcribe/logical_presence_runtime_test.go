package transcribe

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/testdata/castmodel"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGeneratedLogicalRequestPresence(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[castmodel.Signals]())); err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "Events", Scope: "example.com/logical", Connector: "main", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#import('domain','github.com/viant/datly/transcribe/testdata/castmodel')
#setting($_ = $route('/events','PATCH'))
#setting($_ = $case_format('lc'))
#define($_ = $Events<[]*EventsView>(body/data))
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /* SELECT id,name FROM events WHERE id IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end) */)
#define($_ = $Data<[]*EventsView>(output/body))
SELECT e.*,CAST(e.category AS '*string'),CAST(e.labels AS '[]string'),CAST(e.enabled AS bool),CAST(e.count AS int),CAST(e.signals AS '*domain.Signals'),CAST(e.trustedsignals AS '*domain.Signals'),CAST(e.internalfalse AS bool),CAST(e.internalzero AS int),tag(e.category,'sqlx:"-"'),tag(e.labels,'sqlx:"-"'),tag(e.enabled,'sqlx:"-"'),tag(e.count,'sqlx:"-"'),tag(e.signals,'sqlx:"-"'),tag(e.hidden,'sqlx:"-" json:"-"'),tag(e.internal,'sqlx:"-" internal:"true"'),tag(e.hiddenformat,'sqlx:"-" format:"-"'),tag(e.trustedsignals,'sqlx:"-" internal:"true"'),internal_transient(e.internalfalse),internal_transient(e.internalzero)
FROM (SELECT id,name,'' AS category,'' AS labels,0 AS enabled,0 AS count,'' AS signals,'' AS hidden,'' AS internal,'' AS hiddenformat,'' AS trustedsignals,0 AS internalfalse,0 AS internalzero FROM events) e`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Current: "CurrentEvents"}}}
	var first map[string]string
	for i := 0; i < 2; i++ {
		if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{}
		err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files[relative] = string(data)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatal("native generation emitted no Go artifacts")
		}
		if i == 0 {
			first = files
		} else if !reflect.DeepEqual(first, files) {
			t.Fatal("native generated artifacts changed on identical transcription")
		}
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "logical_presence_test.go"), []byte(logicalPresenceRuntimeSource), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-race", "-count=1", "-v", "-mod=mod", "-timeout", "60s", "./...")
	command.Dir = root
	// Fixture-only graph closure retains the original test policy. The actual
	// SDK checkout and the outer test command remain readonly.
	evidence, err := os.MkdirTemp("", "plan31-logical-presence-module-")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"command": command.Args, "directory": root, "sourceModfile": os.Getenv("DATLY_TEST_MODFILE"), "sdkSource": os.Getenv("DATLY_TEST_XDATLY_DIR")}
	archiveLogicalPresenceModule(t, root, evidence, "before", metadata)
	output, runErr := command.CombinedOutput()
	archiveLogicalPresenceModule(t, root, evidence, "after", metadata)
	if err = os.WriteFile(filepath.Join(evidence, "output.log"), output, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("generated logical presence module evidence: %s", evidence)
	if runErr != nil {
		t.Fatalf("generated logical presence: %v\n%s", runErr, output)
	}
}

func archiveLogicalPresenceModule(t *testing.T, root, destination, phase string, metadata map[string]any) {
	t.Helper()
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(destination, phase+"."+name), data, 0600); err != nil {
			t.Fatal(err)
		}
		metadata[phase+"."+name] = map[string]any{"bytes": len(data), "sha256": fmt.Sprintf("%x", sha256.Sum256(data))}
	}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(destination, "metadata.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
}

const logicalPresenceRuntimeSource = `package events
import (
 "context"
 "database/sql"
 "encoding/json"
 "fmt"
 "net/http/httptest"
 "strings"
 "reflect"
 "testing"
 "github.com/viant/bindly/provider/body"
 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/runtime/differ"
 dexec "github.com/viant/datly/exec"
 tformat "github.com/viant/tagly/format"
 gateway "github.com/viant/datly/gateway/http"
 "github.com/viant/datly/gateway/openapi"
 "github.com/viant/datly/gateway/openapi/openapi3"
 mcpinvocation "github.com/viant/datly/mcp/invocation"
 mcptool "github.com/viant/datly/mcp/tool"
 druntime "github.com/viant/datly/runtime"
 rhandler "github.com/viant/datly/runtime/handler"
 customhandler "github.com/viant/datly/runtime/handler/custom"
 dsql "github.com/viant/datly/sql"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 "github.com/viant/mcp-protocol/schema"
 "github.com/viant/datly/sql/dml"
 "github.com/viant/datly/transcribe/testdata/castmodel"
 xhandler "github.com/viant/xdatly/handler"
 _ "github.com/mattn/go-sqlite3"
)
var internalPresenceFields=[]string{"Hidden","Internal","Hiddenformat","Trustedsignals","Internalfalse","Internalzero"}
var privateTransportFields=[]string{"Hidden","Internal","Trustedsignals","Internalfalse","Internalzero"}
func formatLiteralName()string{field,_:=reflect.TypeOf(EventsView{}).FieldByName("Hiddenformat");name:=strings.Split(field.Tag.Get("json"),",")[0];if name==""{name=field.Name};return name}
func TestInternalTransientDiffExclusion(t *testing.T){
 from,to:=&EventsView{},&EventsView{};from.SetInternalfalse(false);to.SetInternalfalse(true);from.SetInternalzero(0);to.SetInternalzero(9)
 changes,err:=differ.New().Diff(context.Background(),from,to);if err!=nil{t.Fatal(err)};if records:=changes.ToChangeRecords();len(records)!=0{t.Fatalf("transient fields appeared in diff: %+v",records)}
}
func TestInternalPresenceFieldMetadata(t *testing.T){
 for _,name:=range []string{"Internalfalse","Internalzero"}{field,ok:=reflect.TypeOf(EventsView{}).FieldByName(name);if !ok{t.Fatal("missing transient field")};for key,want:=range map[string]string{"sqlx":"-","diff":"-","internal":"true","json":"-"}{if field.Tag.Get(key)!=want{t.Fatalf("%s %s=%q",name,key,field.Tag.Get(key))}};if _,ok:=reflect.TypeOf(EventsViewHas{}).FieldByName(name);!ok{t.Fatalf("missing transient Has %s",name)}}
 metadata:=map[string]any{};for _,entry:=range []struct{name string;typ reflect.Type}{{"body",reflect.TypeOf(EventsView{})},{"Has",reflect.TypeOf(EventsViewHas{})}}{var fields []map[string]string;for i:=0;i<entry.typ.NumField();i++{field:=entry.typ.Field(i);fields=append(fields,map[string]string{"name":field.Name,"type":field.Type.String(),"tag":string(field.Tag)})};metadata[entry.name]=fields};data,err:=json.Marshal(metadata);if err!=nil{t.Fatal(err)};t.Logf("BODY_FIELD_METADATA %s",data)
 field,_:=reflect.TypeOf(EventsView{}).FieldByName("Hiddenformat");parsed,err:=tformat.Parse(field.Tag);if err!=nil||parsed.Ignore||parsed.CaseFormat!="-"{t.Fatalf("pinned literal format semantics %+v %v",parsed,err)}
}
func TestInternalPresenceNilHasSetters(t *testing.T){
 for _,field:=range internalPresenceFields{row:=&EventsView{};setter:=reflect.ValueOf(row).MethodByName("Set"+field);if !setter.IsValid(){t.Fatalf("missing trusted setter %s",field)};setter.Call([]reflect.Value{reflect.Zero(setter.Type().In(0))});if row.Has==nil||!reflect.ValueOf(*row.Has).FieldByName(field).Bool(){t.Fatalf("zero setter did not initialize/mark %s",field)}}
}
func TestLogicalPresenceBindingCaptureAndPersistence(t *testing.T){
 ctx:=context.Background()
 for _,tc:=range []struct{name,request string;present bool}{
 {"omitted","{\"id\":1}",false},
 {"null","{\"id\":1,\"category\":null,\"labels\":null,\"enabled\":false,\"count\":0,\"signals\":null}",true},
 {"empty","{\"id\":1,\"category\":\"\",\"labels\":[],\"enabled\":false,\"count\":0,\"signals\":{}}",true},
 {"value","{\"id\":1,\"category\":\"a\",\"labels\":[\"b\"],\"enabled\":true,\"count\":2,\"signals\":{\"Label\":\"client\"}}",true},
 }{t.Run(tc.name,func(t *testing.T){
 provider,err:=body.New([]byte(tc.request),"application/json",nil);if err!=nil{t.Fatal(err)}
 value,found,err:=provider.Value(ctx,reflect.TypeOf(EventsView{}),"");if err!=nil||!found{t.Fatalf("binding: %v",err)}
 row:=value.(EventsView)
 if row.Has==nil||row.Has.Category!=tc.present||row.Has.Labels!=tc.present||row.Has.Enabled!=tc.present||row.Has.Count!=tc.present||row.Has.Signals!=tc.present{t.Fatalf("presence: %+v",row.Has)}
 if tc.name=="empty"&&(row.Enabled||row.Count!=0||row.Signals==nil||row.Signals.Label!=""){t.Fatalf("empty scalar values: %+v",row)}
 if tc.name=="null"&&row.Signals!=nil{t.Fatal("null structured scalar was not nil")}
 if tc.name=="value"&&(!row.Enabled||row.Count!=2||row.Signals==nil||row.Signals.Label!="client"){t.Fatalf("bound scalar values: %+v",row)}
 for _,hidden:=range internalPresenceFields{field,ok:=reflect.TypeOf(*row.Has).FieldByName(hidden);if !ok||field.Type.Kind()!=reflect.Bool{t.Fatalf("hidden scalar marker missing: %s",hidden)};if reflect.ValueOf(*row.Has).FieldByName(hidden).Bool(){t.Fatalf("omitted hidden marker set: %s",hidden)}}
 input:=&EventsInput{Events:[]*EventsView{&row}}
 capturer:=any(NewEventsHandler()).(xhandler.InputCapturer[EventsInput])
 captured,err:=capturer.CaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
 original:=captured.(*_newEventsHandlerOriginalInput).Roots[0]
 for _,field:=range internalPresenceFields{if original.Has(field){t.Fatalf("omitted original hidden marker: %s",field)}}
 fields:=[]string{"Category","Labels","Enabled","Count","Signals"}
 for _,field:=range fields{if original.Has(field)!=tc.present{t.Fatalf("original %s",field)}}
 row.SetCategory(nil);row.SetLabels([]string{"changed"});row.SetEnabled(false);row.SetCount(0);row.SetSignals(&castmodel.Signals{Label:"changed"})
 if !row.Has.Category||!row.Has.Labels||!row.Has.Enabled||!row.Has.Count||!row.Has.Signals{t.Fatal("setters lost logical presence")}
 for _,field:=range fields{if original.Has(field)!=tc.present{t.Fatalf("working mutation changed original %s",field)}}
 row.SetHidden("");row.SetInternal("");row.SetHiddenformat("");row.SetTrustedsignals(nil);row.SetInternalfalse(false);row.SetInternalzero(0)
 for _,field:=range internalPresenceFields{if !reflect.ValueOf(*row.Has).FieldByName(field).Bool(){t.Fatalf("trusted setter did not mark %s",field)};if original.Has(field){t.Fatalf("trusted setter changed original %s",field)}}
 if err=row.SyncPresence(original);err!=nil{t.Fatal(err)}
 for _,field:=range internalPresenceFields{if original.Has(field){t.Fatalf("synchronization changed original %s",field)}}
 for _,field:=range fields{if original.Has(field)!=tc.present{t.Fatalf("synchronization changed original public %s",field)}}
 db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close()
 if _,err=db.Exec("CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT,untouched TEXT);INSERT INTO events VALUES(1,'before','keep');INSERT INTO events VALUES(2,'other','other');CREATE TABLE auxiliary(id INTEGER PRIMARY KEY,label TEXT);INSERT INTO auxiliary VALUES(1,'keep auxiliary')");err!=nil{t.Fatal(err)}
 name:="after";row.SetName(&name)
 data:=dml.NewData(db);if err=data.BeginInvocation();err!=nil{t.Fatal(err)}
 row.SetTrustedsignals(&castmodel.Signals{Label:"trusted auxiliary scalar"})
 if err=data.Update("events",&row);err!=nil{t.Fatal(err)};if err=data.Complete(ctx,nil);err!=nil{t.Fatal(err)}
 var actual string;if err=db.QueryRow("SELECT name FROM events WHERE id=1").Scan(&actual);err!=nil||actual!="after"{t.Fatalf("physical update: %q %v",actual,err)}
 var unchanged string;if err=db.QueryRow("SELECT untouched FROM events WHERE id=1").Scan(&unchanged);err!=nil||unchanged!="keep"{t.Fatalf("omitted physical column: %q %v",unchanged,err)}
 if err=db.QueryRow("SELECT name FROM events WHERE id=2").Scan(&unchanged);err!=nil||unchanged!="other"{t.Fatalf("unrelated row: %q %v",unchanged,err)}
 if err=db.QueryRow("SELECT label FROM auxiliary WHERE id=1").Scan(&unchanged);err!=nil||unchanged!="keep auxiliary"{t.Fatalf("structured scalar acquired DML: %q %v",unchanged,err)}
 })}
}


func(i *EventsInput)Init(context.Context)error{
 for _,row:=range i.Events{if row!=nil{trusted:="hook value";row.SetInternal(trusted);row.SetHidden(trusted);row.SetHiddenformat(trusted);row.SetTrustedsignals(&castmodel.Signals{Label:"auxiliary hook value"});row.SetInternalfalse(false);row.SetInternalzero(0)}}
 return nil
}
func TestInternalPresenceGeneratedMutationPersistence(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT,untouched TEXT);INSERT INTO events VALUES(1,'before','keep');INSERT INTO events VALUES(2,'other','other');CREATE TABLE auxiliary(id INTEGER PRIMARY KEY,label TEXT);INSERT INTO auxiliary VALUES(1,'keep auxiliary')");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeOf(Component{});field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("missing generated component")};tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:"Component",FieldName:field.Name,PackageName:"events",PackagePath:holder.PkgPath(),Tag:tag,InputType:"EventsInput",OutputType:"EventsOutput"}
 component,err:=source.Resolve(reflect.TypeOf(EventsInput{}),reflect.TypeOf(EventsOutput{}));if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(EventsInput{}),OutputType:reflect.TypeOf(EventsOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 entry,err:=artifact.Registration(registry.RegisteredComponent{Handler:customhandler.New[EventsInput,EventsOutput](NewEventsHandler()),Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 for _,tc:=range []struct{body,want string}{{"{\"data\":[{\"id\":1}]}","before"},{"{\"data\":[{\"id\":1,\"name\":\"after\"}]}","after"}}{
  req:=httptest.NewRequest("PATCH","/events",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)}
  result,err:=rt.ExecuteRoute(ctx,"PATCH","/events",scope);scope.Close();if err!=nil{t.Fatal(err)};row:=result.(*EventsOutput).Data[0]
  for _,name:=range internalPresenceFields{if row.Has==nil||!reflect.ValueOf(*row.Has).FieldByName(name).Bool(){t.Fatalf("generated hook setter flag missing %s",name)}}
  var name,untouched string;if err=db.QueryRow("SELECT name,untouched FROM events WHERE id=1").Scan(&name,&untouched);err!=nil||name!=tc.want||untouched!="keep"{t.Fatalf("generated physical update got %q,%q: %v",name,untouched,err)}
  if err=db.QueryRow("SELECT name FROM events WHERE id=2").Scan(&name);err!=nil||name!="other"{t.Fatalf("unrelated row changed %q: %v",name,err)}
  if err=db.QueryRow("SELECT label FROM auxiliary WHERE id=1").Scan(&name);err!=nil||name!="keep auxiliary"{t.Fatalf("structured scalar acquired persistence %q: %v",name,err)}
 }
}
// The wrapper declares an ordinary supported body parameter; its body is the
// actual transcribed EventsView, including generated Has metadata.
type internalTransportInput struct{Data []*EventsView ` + "`" + `parameter:"Data,kind=body,in=data,required" json:"data"` + "`" + `}
func TestInternalPresencePublicHTTPAndMCP(t *testing.T){
 ctx:=context.Background()
 component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:"example.com/logical",Name:"PublicInternalProof"},Settings:&spec.Settings{CaseFormat:"lc"},Routes:[]*spec.Route{{Method:"PATCH",Path:"/presence",MCP:[]*spec.MCPExposure{{Kind:spec.MCPExposureTool,Name:"presence.patch"}}}}}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(internalTransportInput{}),OutputType:reflect.TypeOf([]*EventsView{})});if err!=nil{t.Fatal(err)}
 calls:=0;var violations []string
 entry,err:=artifact.Registration(registry.RegisteredComponent{Handler:rhandler.HandlerFunc(func(_ context.Context,inv rhandler.Invocation)(any,error){
  calls++;input:=inv.Input.(*internalTransportInput);if len(input.Data)!=1{return nil,fmt.Errorf("wrong public body: %+v",input)};row:=input.Data[0]
  if row.Has==nil||!row.Has.Id{return nil,fmt.Errorf("public physical presence lost")}
  for _,name:=range privateTransportFields{value:=reflect.ValueOf(row).Elem().FieldByName(name);if !value.IsZero()||reflect.ValueOf(*row.Has).FieldByName(name).Bool(){violations=append(violations,fmt.Sprintf("%s value=%v marker=%v",name,value.Interface(),reflect.ValueOf(*row.Has).FieldByName(name).Bool()))}}
  if row.Has.Name{violations=append(violations,"forged Has manufactured Name presence")}
  if row.Hiddenformat!="public format client"||!row.Has.Hiddenformat{violations=append(violations,"literal format field lost public value/presence")}
  // Deliberate trusted values must also remain outside projected output.
  trusted:="server secret";row.SetInternal(trusted);row.SetHidden(trusted);row.SetHiddenformat("public format literal");row.SetTrustedsignals(&castmodel.Signals{Label:"server secret"});row.SetInternalfalse(true);row.SetInternalzero(9)
  return []*EventsView{row},nil
 })});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry});if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 doc,err:=(openapi.Generator{}).Generate(ctx,openapi.Request{Info:openapi3.Info{Title:"Internal presence",Version:"1"},Components:[]*registry.RegisteredComponent{entry},Visibility:rt});if err!=nil{t.Fatal(err)}
 // Inspect every object schema, including refs, in both request and output.
 for name,object:=range doc.Components.Schemas{for _,hidden:=range append(append([]string{},privateTransportFields...),"Has"){for property:=range object.Properties{if strings.EqualFold(property,hidden){t.Errorf("public HTTP schema %s exposes %s",name,property)}}}}
 literalSchemas:=0;for _,object:=range doc.Components.Schemas{for property:=range object.Properties{if strings.EqualFold(property,formatLiteralName()){literalSchemas++;if property!=formatLiteralName(){t.Errorf("format:- changed HTTP literal name: %s",property)}}}};if literalSchemas<2{t.Errorf("format:- public field absent from request/output schemas: %d",literalSchemas)}
 payload:=map[string]any{"id":1,"hidden":"client secret","internal":"client secret",formatLiteralName():"public format client","trustedsignals":map[string]any{"Label":"client secret"},"internalfalse":true,"internalzero":7,"Has":map[string]any{"Name":true,"Internal":true},"unknown":"ignored"}
 for _,protocol:=range []string{"HTTP","MCP"}{t.Run(protocol,func(t *testing.T){
  violations=nil;before:=calls
  if protocol=="HTTP"{
   wire,_:=json.Marshal(map[string]any{"data":[]any{payload}});req:=httptest.NewRequest("PATCH","/presence",strings.NewReader(string(wire)));req.Header.Set("Content-Type","application/json");rec:=httptest.NewRecorder();gateway.NewHandler(rt,nil,"").ServeHTTP(rec,req)
   if rec.Code!=200{t.Fatalf("HTTP %d: %s",rec.Code,rec.Body.String())};assertInternalOutputHidden(t,rec.Body.Bytes());assertLiteralFormatOutput(t,rec.Body.Bytes())
  }else{
   contract,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/presence"});if !ok{t.Fatal("MCP route contract absent")}
   plan,err:=mcptool.NewCompiler().Compile(mcptool.Input{Component:artifact.Component.Key,Exposure:component.Routes[0].MCP[0],Contract:contract,OutputType:entry.OutputType,Output:entry.Output,TransportReady:entry.Output.TransportReady(),Documentation:entry.Documentation});if err!=nil{t.Fatal(err)}
   metadata:=plan.Metadata();encoded,err:=json.Marshal(metadata.InputSchema);if err!=nil{t.Fatal(err)};var objects any;if err=json.Unmarshal(encoded,&objects);err!=nil{t.Fatal(err)};assertInternalSchemaHidden(t,objects);assertLiteralFormatSchema(t,objects)
   result,rpcErr:=mcptool.NewHandler(plan,mcpinvocation.New(mcpinvocation.Config{Invoker:rt,Output:func(target dexec.ComponentTarget)mcpinvocation.OutputEncoder{if target.Component.String()!=entry.Component.Key.String(){return nil};return func(ctx context.Context,value any)([]byte,error){encoded,err:=entry.Output.Encode(ctx,"json",value);return encoded.Data,err}}})).Handle(ctx,&schema.CallToolRequest{Params:schema.CallToolRequestParams{Name:"presence.patch",Arguments:map[string]any{"data":[]any{payload}}}})
   if rpcErr!=nil||result==nil||result.IsError!=nil&&*result.IsError{t.Fatalf("MCP result=%+v error=%v",result,rpcErr)};encoded,err=json.Marshal(result);if err!=nil{t.Fatal(err)};assertInternalOutputHidden(t,encoded);assertLiteralFormatOutput(t,encoded)
  }
  if calls!=before+1{t.Fatal("public handler not invoked")};if len(violations)!=0{t.Fatalf("forged hidden input reached public handler: %v",violations)}
 })}
}
func TestInternalPresenceTrustedTypedInvocation(t *testing.T){
 ctx:=context.Background();component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:"example.com/logical",Name:"TrustedInternalProof"},Routes:[]*spec.Route{{Method:"PATCH",Path:"/trusted-presence"}}}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(internalTransportInput{}),OutputType:reflect.TypeOf([]*EventsView{})});if err!=nil{t.Fatal(err)}
 id:=1;row:=&EventsView{};row.SetId(&id);row.SetInternal("trusted internal");row.SetHidden("trusted hidden");row.SetTrustedsignals(&castmodel.Signals{Label:"trusted structured"});row.SetInternalfalse(false);row.SetInternalzero(0)
 expected:=*row;expectedHas:=*row.Has;expected.Has=&expectedHas;calls:=0
 entry,err:=artifact.Registration(registry.RegisteredComponent{Handler:rhandler.HandlerFunc(func(_ context.Context,inv rhandler.Invocation)(any,error){calls++;input,ok:=inv.Input.(*internalTransportInput);if !ok||len(input.Data)!=1{return nil,fmt.Errorf("typed input changed: %T",inv.Input)};if !reflect.DeepEqual(input.Data[0],&expected){return nil,fmt.Errorf("trusted internal value or marker changed: %+v",input.Data[0])};return input.Data,nil})});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry});if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 result,err:=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:component.Key,Route:spec.RouteRef{Method:"PATCH",Path:"/trusted-presence"}},Input:&internalTransportInput{Data:[]*EventsView{row}}});if err!=nil{t.Fatal(err)}
 if calls!=1||!reflect.DeepEqual(result,[]*EventsView{&expected}){t.Fatalf("trusted typed result/flags changed: calls=%d value=%+v",calls,result)}
}
func assertInternalSchemaHidden(t *testing.T,value any){t.Helper();switch object:=value.(type){case map[string]any:for key,item:=range object{if key=="properties"{if fields,ok:=item.(map[string]any);ok{for name:=range fields{for _,hidden:=range append(append([]string{},privateTransportFields...),"Has"){if strings.EqualFold(name,hidden){t.Errorf("MCP schema exposes %s",name)}}}}};assertInternalSchemaHidden(t,item)};case []any:for _,item:=range object{assertInternalSchemaHidden(t,item)}}}
func assertInternalOutputHidden(t *testing.T,data []byte){t.Helper();var value any;if err:=json.Unmarshal(data,&value);err!=nil{t.Fatal(err)};check:=func(object map[string]any){for name:=range object{for _,hidden:=range append(append([]string{},privateTransportFields...),"Has"){if strings.EqualFold(name,hidden){t.Errorf("public output exposes %s: %s",name,data)}}}};var walk func(any);walk=func(value any){switch object:=value.(type){case map[string]any:check(object);for _,item:=range object{walk(item)};case []any:for _,item:=range object{walk(item)};case string:var decoded any;if json.Unmarshal([]byte(object),&decoded)==nil{walk(decoded)}}};walk(value);if strings.Contains(string(data),"server secret"){t.Errorf("trusted internal value leaked in public output: %s",data)}}
func assertLiteralFormatSchema(t *testing.T,value any){t.Helper();found:=false;var walk func(any);walk=func(value any){switch object:=value.(type){case map[string]any:for key,item:=range object{if key=="properties"{if fields,ok:=item.(map[string]any);ok{for name:=range fields{if strings.EqualFold(name,formatLiteralName()){found=true;if name!=formatLiteralName(){t.Errorf("format:- changed MCP literal name: %s",name)}}}}};walk(item)};case []any:for _,item:=range object{walk(item)}}};walk(value);if !found{t.Error("format:- public literal absent from MCP schema")}}
func assertLiteralFormatOutput(t *testing.T,data []byte){t.Helper();var value any;if err:=json.Unmarshal(data,&value);err!=nil{t.Fatal(err)};found:=false;var walk func(any);walk=func(value any){switch object:=value.(type){case map[string]any:for name,item:=range object{if strings.EqualFold(name,formatLiteralName()){found=true;if name!=formatLiteralName()||item!="public format literal"{t.Errorf("format:- literal output changed: %s=%v",name,item)}};walk(item)};case []any:for _,item:=range object{walk(item)};case string:var decoded any;if json.Unmarshal([]byte(object),&decoded)==nil{walk(decoded)}}};walk(value);if !found{t.Error("format:- public literal absent from output")}
}

`
