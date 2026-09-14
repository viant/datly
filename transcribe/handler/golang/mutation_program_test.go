package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedMutationProgramSQLite(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Table = "records"
	semantic.Root.Sequence = nil // Both request identities are deliberately supplied.
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	semantic.Root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	config := Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	asset, err := MutationProgram(semantic, config)
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
	source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(programSQLiteFixture)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}

const programSQLiteFixture = `package events
import (
 "context";"errors";"reflect";"testing";"time"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
)
type Marker struct{Id,Name bool}
` + "type Record struct{Id *int64 `sqlx:\"id,primaryKey\"`;Name string `sqlx:\"name\"`;Has *Marker `setMarker:\"true\" sqlx:\"-\"`}\n" +
	"type Previous struct{Id *int64 `sqlx:\"id\"`;Name string `sqlx:\"name\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents []*Previous;Mode string}
type Output struct{Data []*Record}
func(i *Input)Init(context.Context)error{*i.Events[0].Id=7;for _,row:=range i.CurrentEvents{*row.Id=99;row.Name="corrupted"};return nil}
` + "type Hooks struct{Input *Input `bind:\"kind=input\"`;initialized,sequenced,queued int}\n" + `
var callbackError=errors.New("hook failed")
var outcomes []handler.Outcome
var finalizerKinds []string
type completion struct{}
func(*completion)Finalize(_ context.Context,_ *Input,_ *Output,outcome handler.Outcome)error{outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,"definition");return nil}
func(h *Hooks)Init(ctx context.Context,row *Record,state handler.EntityState[Record,handler.NoParent])error{
 if h.Input==nil||!state.Original.Has("Id")||!state.Original.Has("Name"){return errors.New("canonical input or presence lost")}
 if state.Previous!=nil&&(*state.Previous.Id!=1||state.Previous.Name!="old"){return errors.New("previous snapshot changed")}
 h.initialized++;return nil
}
func(h *Hooks)Validate(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{if h.initialized!=2{return errors.New("hook lifetime differs")};if h.Input.Mode=="validate"{return callbackError};return nil}
func(h *Hooks)AfterSequence(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{h.sequenced++;return nil}
func(h *Hooks)AfterQueue(_ context.Context,row *Record,_ handler.EntityState[Record,handler.NoParent])error{
 if h.sequenced!=2{return errors.New("sequence hook order lost")};h.queued++
 if h.Input.Mode=="queue"{return callbackError};if h.Input.Mode=="mutate queued"{*row.Id=88};return nil
}
func(h *Hooks)Finalize(ctx context.Context,input *Input,output *Output,outcome handler.Outcome)error{
 if input!=h.Input{return errors.New("finalizer input differs")}
 if outcome.CommitConfirmed()&&(h.queued!=2||len(output.Data)!=2||*output.Data[0].Id!=1||*output.Data[1].Id!=2){return errors.New("finalizer ran before completed output")}
 outcomes=append(outcomes,outcome.Clone());finalizerKinds=append(finalizerKinds,"root");return nil
}
func TestProgram(t *testing.T){
 for _,mode:=range []string{"success","override","binding","validate","queue","mutate queued"}{t.Run(mode,func(t *testing.T){
  outcomes=nil;finalizerKinds=nil;ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(1,'old')");err!=nil{t.Fatal(err)}
  bindings:=[]bindly.BindingSpec{{Path:"CurrentEvents",Name:"CurrentEvents",Location:bindstate.Location{Kind:"view",In:"Current"}},{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}},{Path:"Mode",Name:"Mode",Location:bindstate.Location{Kind:"test",In:"mode"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
  bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
  component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"Current"}}},Views:[]*spec.View{{Name:"Current",Source:&spec.ViewSource{SQL:"SELECT id,name FROM records"}}}}
  dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:inputType,Bindings:bindings});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:projection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
  ref:=spec.RouteRef{Method:"PATCH",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
  one,two:=int64(1),int64(2);events:=[]*Record{{Id:&one,Name:"updated",Has:&Marker{Id:true,Name:true}},{Id:&two,Name:"inserted",Has:&Marker{Id:true,Name:true}}}
  definition:={{FACTORY}}().(*{{DEFINITION}})
  var supplied any=events
  if mode=="override"||mode=="binding"{definition.Finalizer=&completion{}}
  if mode=="binding"{supplied="not a list"}
  result,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{views,values.New("test",map[string]any{"events":supplied,"mode":mode})}})
  success:=mode=="success"||mode=="override"
  if (err==nil)!=success{t.Fatalf("execution error=%v",err)}
  if len(outcomes)!=1||outcomes[0].CommitConfirmed()!=success{t.Fatalf("outcomes=%+v error=%v",outcomes,err)}
  kind:="root";if mode=="override"||mode=="binding"{kind="definition"};if len(finalizerKinds)!=1||finalizerKinds[0]!=kind{t.Fatalf("finalizers=%v",finalizerKinds)}
  type row struct{Id int64;Name string}
  expected:=[]row{{Id:1,Name:"old"}}
  if success{expected=[]row{{Id:1,Name:"updated"},{Id:2,Name:"inserted"}};output:=result.(*Output);if len(output.Data)!=2||output.Data[0]!=events[0]||*output.Data[0].Id!=1||*output.Data[1].Id!=2{t.Fatal("output is not reconciled request body")}}
  if mode=="validate"||mode=="queue"{if !errors.Is(err,callbackError){t.Fatalf("error cause lost: %v",err)}}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records ORDER BY id"},expected)
 })}
}
func TestCaptureFailureCompletion(t *testing.T){
 outcomes=nil;finalizerKinds=nil;one:=int64(1)
 definition:={{FACTORY}}().(*{{DEFINITION}});definition.Finalizer=&completion{}
 input:=&Input{Events:[]*Record{{Id:&one,Has:&Marker{Id:true}}}}
 program,err:=definition.Capture(context.Background(),input)
 if err==nil||program==nil{t.Fatalf("missing read evidence capture=%v error=%v",program,err)}
 ctx,cancel:=context.WithCancel(context.Background());cancel()
 if failure:=program.Finalize(ctx,handler.Outcome{Error:err});failure!=nil{t.Fatal(failure)}
 if len(outcomes)!=1||outcomes[0].Error==nil||outcomes[0].CommitConfirmed(){t.Fatalf("outcomes=%+v",outcomes)}
 if failure:=program.Finalize(ctx,handler.Outcome{Error:err});failure==nil||len(outcomes)!=1{t.Fatal("finalizer repeated")}
}
`
