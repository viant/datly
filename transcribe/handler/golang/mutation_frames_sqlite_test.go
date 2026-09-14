package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"go/ast"
	"strings"
	"testing"
)

func TestMutationFramesThroughEngineSQLite(t *testing.T) {
	runMutationFramesThroughEngineSQLite(t, false)
}

func TestMutationInvariantsThroughEngineSQLite(t *testing.T) {
	runMutationFramesThroughEngineSQLite(t, true)
}

func runMutationFramesThroughEngineSQLite(t *testing.T, invariant bool) {
	t.Helper()
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "*string"}, Writable: true},
	}}
	semantic.Root.Current.Fields = []plan.CurrentField{
		{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect},
		{Current: plan.FieldRef{Field: "LoadedName", Type: spec.TypeRef{Name: "*string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "*string"}}, Conversion: plan.LinkDirect},
	}
	if invariant {
		semantic.Root.Entity.Fields = append(semantic.Root.Entity.Fields, plan.EntityField{Name: "Enabled", Type: spec.TypeRef{Name: "bool"}, Writable: true})
		semantic.Root.Entity.Invariants = []plan.InvariantGroup{{Name: "Settings", Fields: []string{"Name", "Enabled"}}}
	}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	config := Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := MutationFrameSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	hooks, err := MutationHookSupport(semantic, config, frames.Layout)
	if err != nil {
		t.Fatal(err)
	}
	source := frameSQLiteFixture
	products := []*ast.File{frames.File, frames.Previous.File, frames.Layout.File, hooks.File}
	if invariant {
		phase, err := MutationInvariantSupport(semantic, config, entities, frames.Layout)
		if err != nil {
			t.Fatal(err)
		}
		products = append(products, phase.File)
		replacements := []string{
			"Id,Name bool", "Id,Name,Enabled bool",
			"Evidence bool}", "Evidence bool;Enabled bool}",
			"Has:&Marker{Id:true}", "Enabled:true,Has:&Marker{Id:true,Enabled:true}",
			"if err=hooks.Init(ctx,frames)", "if err=" + phase.Function + "(ctx,frames);err!=nil{return err};if err=hooks.Init(ctx,frames)",
			"current.Evidence=state.PreviousFields.Has", "if !current.Enabled||current.Has.Name{return fmt.Errorf(\"invariant changed supplied value or marker\")};if (current.Name==nil)!=(state.Previous.Name==nil){return fmt.Errorf(\"invariant did not run before Init\")};if current.Name!=nil&&*current.Name!=*state.Previous.Name{return fmt.Errorf(\"backfilled value differs\")};current.Evidence=state.PreviousFields.Has",
			"}});if err!=nil{t.Fatal(err)}\n   got:=actual", "}});if !tc.loaded{if err==nil||!strings.Contains(err.Error(),\"was not loaded\"){t.Fatalf(\"error=%v\",err)};for _,event:=range events{if event.Has.Name{t.Fatal(\"hook ran after invariant failure\")}};return};if err!=nil{t.Fatal(err)}\n   got:=actual",
			"\"context\";\"fmt\";\"reflect\";\"testing\"", "\"context\";\"fmt\";\"reflect\";\"testing\";\"strings\"",
		}
		for index := 0; index < len(replacements); index += 2 {
			expected := 1
			if replacements[index] == "Has:&Marker{Id:true}" {
				expected = 2
			}
			if actual := strings.Count(source, replacements[index]); actual != expected {
				t.Fatalf("invariant fixture anchor %q occurs %d times, expected %d", replacements[index], actual, expected)
			}
		}
		source = strings.NewReplacer(replacements...).Replace(source)
	}
	runEntitySyncFixture(t, semantic, types, source, products...)
}

const frameSQLiteFixture = `package events
import (
 "context";"fmt";"reflect";"testing"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/custom";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
)
type Marker struct{Id,Name bool}
type Record struct{Id *int64;Name *string;Has *Marker;Evidence bool}
` + "type Previous struct{Id *int64 `sqlx:\"id\"`;LoadedName *string `sqlx:\"loaded_name\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents []*Previous;initialized bool}
type Output struct{Data []*Record}
func(i *Input)Init(ctx context.Context)error{for _,r:=range i.CurrentEvents{*r.Id=99;r.LoadedName=nil};i.initialized=true;return nil}
type Hooks struct{initialized,sequenced,queued int}
func(h *Hooks)Init(ctx context.Context,current *Record,state handler.EntityState[Record,handler.NoParent])error{
 if state.Previous==nil{return fmt.Errorf("missing database Previous")}
 if !state.Original.Has("Id")||state.Original.Has("Name"){return fmt.Errorf("incorrect original presence")}
 current.Evidence=state.PreviousFields.Has("Name")
 if state.PreviousFields.Has("LoadedName"){return fmt.Errorf("SQL DTO name leaked to entity fields")}
 current.SetName(state.Previous.Name);h.initialized++;return nil
}
func(h *Hooks)Validate(ctx context.Context,current *Record,state handler.EntityState[Record,handler.NoParent])error{
 if h.initialized!=2||!current.Has.Name{return fmt.Errorf("hook instance or setter presence lost")};return nil
}
func(h *Hooks)AfterSequence(ctx context.Context,current *Record,state handler.EntityState[Record,handler.NoParent])error{
 if h.initialized!=2||state.Previous==nil||!state.Original.Has("Id"){return fmt.Errorf("AfterSequence lost hook or frame state")};h.sequenced++;return nil
}
func(h *Hooks)AfterQueue(ctx context.Context,current *Record,state handler.EntityState[Record,handler.NoParent])error{
 if h.sequenced!=2||state.Previous==nil{return fmt.Errorf("AfterQueue lost hook or frame state")};h.queued++;return nil
}
type captured struct{original *_newEventsHandlerOriginalInput;database *_newEventsHandlerDatabaseSnapshot}
type contract struct{}
func(*contract)RequiresReadMetadata()bool{return true}
func(*contract)CaptureInput(ctx context.Context,input *Input)(any,error){
 if input.initialized{return nil,fmt.Errorf("capture too late")}
 metadata,ok:=handler.ReadMetadataFromContext(ctx);if !ok{return nil,fmt.Errorf("missing read metadata")}
 original,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{return nil,err}
 database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata);if err!=nil{return nil,err}
 return &captured{original:original.(*_newEventsHandlerOriginalInput),database:database},nil
}
func(*contract)Exec(ctx context.Context,session handler.Session,input *Input,output *Output)error{
 if !input.initialized{return fmt.Errorf("input initializer skipped")}
 snapshot,found,err:=session.Binder().Lookup(ctx,handler.InputSnapshotKey);if err!=nil{return err};if !found{return fmt.Errorf("missing snapshot")}
 value:=snapshot.(*captured);sync,err:=value.original.synchronize(input);if err!=nil{return err}
 frames,err:=value.database.Build(input,sync);if err!=nil{return err}
 hooks:=&_newEventsHandlerMutationHooks{};if err=hooks.Prepare(ctx,session.Binder());err!=nil{return err}
 if err=hooks.Init(ctx,frames);err!=nil{return err};if err=hooks.Validate(ctx,frames);err!=nil{return err}
 if err=hooks.AfterQueue(ctx,frames);err==nil{return fmt.Errorf("AfterQueue ran before AfterSequence")}
 if err=hooks.AfterSequence(ctx,frames);err!=nil{return err};if err=hooks.AfterQueue(ctx,frames);err!=nil{return err}
 if hooks.hook0.queued!=2{return fmt.Errorf("AfterQueue callback count differs")}
 if err=hooks.AfterSequence(ctx,frames);err==nil{return fmt.Errorf("AfterSequence repeated")};if err=hooks.AfterQueue(ctx,frames);err==nil{return fmt.Errorf("AfterQueue repeated")}
 output.Data=input.Events;return nil
}
func TestEngineFrames(t *testing.T){
 for _,tc:=range []struct{name,query string;loaded bool}{{"selected","SELECT id,name AS loaded_name FROM records ORDER BY id",true},{"omitted","SELECT id FROM records ORDER BY id",false}}{
  t.Run(tc.name,func(t *testing.T){
   ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(0,'retained'),(1,NULL)");err!=nil{t.Fatal(err)}
   bindings:=[]bindly.BindingSpec{{Path:"CurrentEvents",Name:"CurrentEvents",Location:bindstate.Location{Kind:"view",In:"Current"}},{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}}}
   seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
   plan,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=plan.Projection();if err!=nil{t.Fatal(err)}
   allowNulls:=true
   component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"Current"}}},Views:[]*spec.View{{Name:"Current",AllowNulls:&allowNulls,Source:&spec.ViewSource{SQL:tc.query}}}}
   dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:inputType,Bindings:bindings});if err!=nil{t.Fatal(err)}
   views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:projection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
   ref:=spec.RouteRef{Method:"PATCH",Path:"/records"};inputContract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:plan,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=inputContract.ForRoute(ref);if !ok{t.Fatal("route missing")}
   zero,one:=int64(0),int64(1);events:=[]*Record{{Id:&zero,Has:&Marker{Id:true}},{Id:&one,Has:&Marker{Id:true}}}
   actual,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:custom.New[Input,Output](&contract{}),Providers:[]locator.Provider{views,values.New("test",map[string]any{"events":events})}});if err!=nil{t.Fatal(err)}
   got:=actual.(*Output).Data;if len(got)!=2||*got[0].Id!=0||*got[1].Id!=1{t.Fatalf("IDs %v",got)}
   for _,r:=range got{if r.Evidence!=tc.loaded{t.Fatalf("actual PreviousFields loaded=%v want%v",r.Evidence,tc.loaded)}}
   if tc.loaded&&(got[0].Name==nil||*got[0].Name!="retained"){t.Fatal("pre-Init database snapshot lost")};if got[1].Name!=nil{t.Fatal("loaded NULL changed")}
  })
 }
}
`
