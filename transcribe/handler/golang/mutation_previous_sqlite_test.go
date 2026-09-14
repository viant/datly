package golang

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedPreviousUsesSQLiteReadEvidence(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPatch, plan.OperationPut} {
		t.Run(string(operation), func(t *testing.T) {
			testGeneratedPreviousUsesSQLiteReadEvidence(t, operation)
		})
	}
}

func testGeneratedPreviousUsesSQLiteReadEvidence(t *testing.T, operation plan.Operation) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Operation = operation
	semantic.Root.Write = fixtureWritePolicy(operation, semantic.Root.InputPath, 0)
	if operation == plan.OperationPut {
		semantic.Root.Sequence = nil
	}
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "*string"}, Writable: true},
	}}
	semantic.Root.Current.Fields = []plan.CurrentField{
		{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect},
		{Current: plan.FieldRef{Field: "LoadedName", Type: spec.TypeRef{Name: "*string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "*string"}}, Conversion: plan.LinkDirect},
	}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := MutationPreviousSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, previousSQLiteFixture, previous.File)
}

const previousSQLiteFixture = `package events
import (
 "context"; "reflect"; "strings"; "testing"
 "github.com/viant/bindly"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader/compiler"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
)
type Marker struct{Id,Name bool}
type Record struct{Id *int64;Name *string;Has *Marker}
` + "type Previous struct{Id *int64 `sqlx:\"id\"`;LoadedName *string `sqlx:\"loaded_name\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents []*Previous}
type Output struct{Data []*Record}
func TestPreviousRead(t *testing.T){
 for _,tc:=range []struct{name,query,wantError string;loaded bool}{
  {"loaded null","SELECT id,name AS loaded_name FROM records ORDER BY id","",true},
  {"omitted name","SELECT id FROM records ORDER BY id","",false},
  {"missing key","SELECT name AS loaded_name FROM records ORDER BY id","was not loaded",true},
  {"duplicate key","SELECT id,name AS loaded_name FROM records UNION ALL SELECT id,name AS loaded_name FROM records","duplicate",true},
 }{t.Run(tc.name,func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(0,'retained'),(1,NULL)");err!=nil{t.Fatal(err)}
  bindings:=[]bindly.BindingSpec{{Path:"CurrentEvents",Name:"CurrentEvents",Location:bindstate.Location{Kind:"view",In:"Current"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)}
  typ:=reflect.TypeOf(Input{});boundPlan,err:=seed.CompilePlan(typ,bindings...);if err!=nil{t.Fatal(err)}
  inputProjection,err:=boundPlan.Projection();if err!=nil{t.Fatal(err)}
  allowNulls:=true
  component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"Current"}}},Views:[]*spec.View{{Name:"Current",AllowNulls:&allowNulls,Source:&spec.ViewSource{SQL:tc.query}}}}
  dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:typ,Bindings:bindings});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:inputProjection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
  injector,err:=seed.ForScope(views);if err!=nil{t.Fatal(err)}
  input:=&Input{};var projection handler.ReadProjection
  err=injector.Bind(ctx,input,bindly.WithPlan(boundPlan),bindly.WithBindingObserver(func(_ context.Context,event bindly.BindingEvent)error{
   if event.Target!=input||event.Path!="CurrentEvents"{t.Fatalf("unexpected binding %+v",event)}
   projection,_=event.Metadata.(handler.ReadProjection);return nil
  }));if err!=nil{t.Fatal(err)}
  snapshot,err:=_newEventsHandlerPrevious0Capture(input.CurrentEvents,projection)
  if tc.wantError!=""{if err==nil||!strings.Contains(err.Error(),tc.wantError)||snapshot!=nil{t.Fatalf("snapshot=%v error=%v",snapshot,err)};return}
  if err!=nil{t.Fatal(err)}
  for _,id:=range []int64{0,1}{
   row:=snapshot.byKey[_newEventsHandlerMatchKey0{Id:id}];if row==nil||!row.fields.Has("Id")||row.fields.Has("Name")!=tc.loaded||row.fields.Has("LoadedName"){t.Fatalf("wrong previous evidence id=%d row=%+v",id,row)}
   if id==1&&row.value.Name!=nil{t.Fatal("SQL NULL lost")}
  }
  if tc.loaded{
   *input.CurrentEvents[0].LoadedName="changed";*input.CurrentEvents[0].Id=99
   if *snapshot.byKey[_newEventsHandlerMatchKey0{}].value.Name!="retained"||*snapshot.byKey[_newEventsHandlerMatchKey0{}].value.Id!=0{t.Fatal("Previous aliases live Current")}
  } else if snapshot.byKey[_newEventsHandlerMatchKey0{}].value.Name!=nil{t.Fatal("omitted field populated")}
 })}
}
`
