package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedMutationErrorPolicySQLiteAndWire(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Table = "records"
	semantic.Root.Sequence = nil
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	semantic.Root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
	asset, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Record", "[]*Previous")})
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	var products []*ast.File
	for _, file := range files {
		if file != asset.Entities.File {
			products = append(products, file)
		}
	}
	// Reuse the executable Program fixture, changing only authored application
	// policy and assertions. Generated production syntax is never rewritten.
	source := programSQLiteFixture
	for _, edit := range [][2]string{
		{"\"context\";\"errors\";", "\"context\";\"errors\";\"encoding/json\";\"fmt\";\"strings\";\"net/http/httptest\";"},
		{"\"github.com/viant/datly/internal/testharness/sqlite\"", `"github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/internal/testharness/mcpclient"
 "github.com/viant/datly/bootstrap"
 gateway "github.com/viant/datly/gateway/http"
 dmcp "github.com/viant/datly/mcp"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/mcp-protocol/schema"
 "github.com/viant/xdatly/response"`},
		{"Name string `sqlx:\"name\"`", "Name string `sqlx:\"name\" validate:\"required\"`"},
		{"type Input struct{Events []*Record;CurrentEvents []*Previous;Mode string}", "type Input struct{Events []*Record `json:\"events\"`;CurrentEvents []*Previous `json:\"-\"`;Mode string `json:\"mode\"`}"},
		{"var outcomes []handler.Outcome", "var outcomes []handler.Outcome\nvar validationCalls int"},
		{"func(h *Hooks)Validate(_ context.Context,_ *Record,state handler.LifecycleContext[Record,handler.NoParent,Output])error{", "func(h *Hooks)Validate(_ context.Context,_ *Record,state handler.LifecycleContext[Record,handler.NoParent,Output])error{validationCalls++;"},
		{"func(*completion)Finalize(_ context.Context,_ *Input,_ *Output,outcome handler.Outcome)error{outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,\"definition\");return nil}", "func(*completion)Finalize(_ context.Context,input *Input,_ *Output,outcome handler.Outcome)error{outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,\"definition\");return policyFailure(input,outcome)}"},
		{"outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,\"root\");return nil", "if strings.HasPrefix(input.Mode,\"framework\")&&(h.sequenced!=0||h.queued!=0){return errors.New(\"write phases ran after failed validation\")};outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,\"root\");return policyFailure(input,outcome)"},
		{"[]string{\"success\",\"override\",\"binding\",\"validate\",\"queue\",\"mutate queued\"}", "[]string{\"success\",\"framework root\",\"framework definition\",\"framework null\",\"framework empty\",\"framework plain\",\"framework wrapped\"}"},
		{"outcomes=nil;finalizerKinds=nil;ctx,cancel:=", "outcomes=nil;finalizerKinds=nil;validationCalls=0;ctx,cancel:="},
		{"  var supplied any=events", "  if strings.HasPrefix(mode,\"framework\"){events[0].Name=\"\";events[1].Name=\"\"};var supplied any=events"},
		{"mode==\"override\"||mode==\"binding\"", "mode==\"override\"||mode==\"binding\"||mode==\"framework definition\""},
		{"success:=mode==\"success\"||mode==\"override\"", "if strings.HasPrefix(mode,\"framework\"){assertPolicyError(t,mode,err)};success:=mode==\"success\"||mode==\"override\""},
	} {
		if !strings.Contains(source, edit[0]) {
			t.Fatalf("Program fixture anchor missing: %s", edit[0])
		}
		source = strings.ReplaceAll(source, edit[0], edit[1])
	}
	source = strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(source + mutationErrorPolicyFixture)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}

const mutationErrorPolicyFixture = `
var policyPrivate=errors.New("PRIVATE database cause")
var policyObserved chan handler.Outcome

func policyBody(mode string)any{
 switch mode{case "framework null":return nil;case "framework empty":return ""}
 return map[string]any{"message":"","error":nil,"violations":[]any{}}
}
func policyFailure(input *Input,outcome handler.Outcome)error{
 if input==nil||!strings.HasPrefix(input.Mode,"framework"){return nil}
 var failed *handler.Validation
 if !errors.As(outcome.Error,&failed)||len(failed.Violations)!=2||validationCalls!=0||outcome.State()!=handler.TransactionNone{ return errors.New("mandatory framework validation evidence lost or transaction started") }
 if policyObserved!=nil{policyObserved<-outcome.Clone()}
 switch input.Mode{
 case "framework plain":return policyPrivate
 case "framework wrapped":return fmt.Errorf("PRIVATE cleanup: %w",outcome.Error)
 }
 return &response.Error{Code:401,Payload:policyBody(input.Mode),Cause:errors.Join(outcome.Error,policyPrivate)}
}
func assertPolicyError(t *testing.T,mode string,err error){
 t.Helper();var failed *handler.Validation;var diagnostic *engine.FinalizationError
 if !errors.As(err,&failed)||len(failed.Violations)!=2||validationCalls!=0||!errors.Is(err,failed)||!errors.As(err,&diagnostic)||diagnostic.Outcome.CommitConfirmed(){t.Fatalf("error=%v custom validation calls=%d",err,validationCalls)}
 body,explicit:=response.ErrorBody(err);code:=response.ErrorStatusCode(err,0)
 if mode=="framework plain"||mode=="framework wrapped"{if !explicit||body!=failed||code!=422{t.Fatalf("cleanup changed projection: %v",err)};return}
 if !explicit||code!=401||!reflect.DeepEqual(body,policyBody(mode))||!errors.Is(err,policyPrivate){t.Fatalf("mapping status=%d body=%#v error=%v",code,body,err)}
}

func TestGeneratedPolicyWire(t *testing.T){
 for _,protocol:=range []string{"HTTP","MCP invocation","MCP wire"}{for _,mode:=range []string{"framework root","framework definition","framework null","framework empty"}{t.Run(protocol+"/"+mode,func(t *testing.T){
  if protocol=="MCP wire"&&testing.Short(){t.Skip("native MCP wire requires a loopback listener")}
  ctx:=context.Background();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(1,'old')");err!=nil{t.Fatal(err)}
  outcomes=nil;finalizerKinds=nil;validationCalls=0;policyObserved=make(chan handler.Outcome,1)
  t.Cleanup(func(){policyObserved=nil})
  component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:"example.com/policy",Name:"Patch"},Routes:[]*spec.Route{{Method:"PATCH",Path:"/records",MCP:[]*spec.MCPExposure{{Kind:spec.MCPExposureTool,Name:"records.patch"}}}},Parameters:[]*spec.Parameter{
   {Name:"Events",Source:spec.BindSource{Kind:"body",Name:"events"}},
   {Name:"Mode",Source:spec.BindSource{Kind:"body",Name:"mode"}},
   {Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"Current"}},
  },Views:[]*spec.View{{Name:"Current",Source:&spec.ViewSource{SQL:"SELECT id,name FROM records"}}}}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(Input{}),OutputType:reflect.TypeOf(Output{})});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
  definition:={{FACTORY}}().(*{{DEFINITION}});if mode=="framework definition"{definition.Finalizer=&completion{}}
  registered:=&registry.RegisteredComponent{Component:artifact.Component,Input:artifact.Input,OutputType:reflect.TypeOf(Output{}),Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{views}}
  runtime,err:=druntime.NewRuntime([]*registry.RegisteredComponent{registered});if err!=nil{t.Fatal(err)}
  args:=map[string]any{"mode":mode,"events":[]any{map[string]any{"Id":1,"Name":""},map[string]any{"Id":2,"Name":""}}}
  want,err:=json.Marshal(policyBody(mode));if err!=nil{t.Fatal(err)}
  if protocol=="HTTP"{
   body,err:=json.Marshal(args);if err!=nil{t.Fatal(err)}
   request:=httptest.NewRequest("PATCH","/records",strings.NewReader(string(body)));request.Header.Set("Content-Type","application/json")
   recorder:=httptest.NewRecorder();gateway.NewHandler(runtime,nil,"policy-test").ServeHTTP(recorder,request)
   if recorder.Code!=401||strings.TrimSpace(recorder.Body.String())!=string(want){t.Fatalf("HTTP status=%d body=%s want=%s",recorder.Code,recorder.Body.String(),want)}
  }else{
   service,err:=dmcp.New(dmcp.Config{Components:[]*registry.RegisteredComponent{registered},Invoker:runtime});if err!=nil{t.Fatal(err)}
   var result *schema.CallToolResult
   if protocol=="MCP wire"{
    native:=mcpclient.New(t,service,schema.LatestProtocolVersion)
    result,err=native.CallTool(ctx,&schema.CallToolRequestParams{Name:"records.patch",Arguments:args})
   }else{
    entry,ok:=service.Registry().ToolRegistry.Get("records.patch");if !ok{t.Fatal("MCP tool missing")}
    actual,failure:=entry.Handler(ctx,&schema.CallToolRequest{Method:schema.MethodToolsCall,Params:schema.CallToolRequestParams{Name:"records.patch",Arguments:args}})
    if failure!=nil{t.Fatal(failure)};result=actual
   }
   if err!=nil||result==nil||result.IsError==nil||!*result.IsError{t.Fatalf("MCP result=%+v error=%v",result,err)}
   encoded,err:=json.Marshal(result);if err!=nil{t.Fatal(err)}
   if strings.Contains(string(encoded),"PRIVATE")||strings.Contains(string(encoded),"Data"){t.Fatalf("private/output content leaked: %s",encoded)}
   var wire struct{Content []struct{Text string};Structured json.RawMessage ` + "`json:\"structuredContent\"`" + `}
   if err:=json.Unmarshal(encoded,&wire);err!=nil{t.Fatal(err)}
   if len(wire.Content)!=1||wire.Content[0].Text!=string(want){t.Fatalf("MCP content=%s want=%s",encoded,want)}
   if mode=="framework null"||mode=="framework empty"{if len(wire.Structured)>0&&string(wire.Structured)!="null"{t.Fatalf("non-object acquired structured body: %s",encoded)}}else{
    var actual,expected any
    if err:=json.Unmarshal(wire.Structured,&actual);err!=nil{t.Fatal(err)};if err:=json.Unmarshal(want,&expected);err!=nil{t.Fatal(err)}
    if !reflect.DeepEqual(actual,expected){t.Fatalf("MCP structured=%s want=%s",wire.Structured,want)}
   }
  }
  select{case outcome:=<-policyObserved:
   if outcome.CommitConfirmed()||outcome.Error==nil{t.Fatalf("mapped failure succeeded: %+v",outcome)}
  default:t.Fatal("configured finalizer was not called")}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{Id int64;Name string}{{1,"old"}})
 })}}
}
`
