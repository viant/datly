package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

type partialIdentityFixture struct{ operation plan.Operation }

func (f partialIdentityFixture) semantic() *plan.Plan {
	semantic := rootSemanticPlan(f.operation, true)
	root := semantic.Root
	root.Table = "records"
	root.Sequence = &plan.SequencePlan{Destination: root.InputPath, Selector: plan.FieldPath{"Id"}, Field: plan.FieldRef{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "*int64"}}}
	root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "TenantId", Type: spec.TypeRef{Name: "int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
	}}
	if f.operation != plan.OperationPost {
		root.Current = &plan.CurrentPlan{ParamIdentity: "currentevents|view|currentevents", ViewIdentity: currentEventsViewIdentity, InputPath: plan.FieldPath{"Input", "CurrentEvents"}, Keys: cloneFixtureKeys(root.Keys)}
		for _, field := range root.Entity.Fields {
			root.Current.Fields = append(root.Current.Fields, plan.CurrentField{Current: plan.FieldRef{Field: field.Name, Type: field.Type}, Entity: plan.FieldRef{Field: field.Name, Type: field.Type}, Conversion: plan.LinkDirect})
		}
	}
	return semantic
}

func (f partialIdentityFixture) config(semantic *plan.Plan) Config {
	previous := ""
	if semantic.Root.Current != nil {
		previous = "[]*Previous"
	}
	return Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Record", previous)}
}

func TestMutationProgramPartialSequenceIdentitySQLite(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPost, plan.OperationPatch, plan.OperationPut} {
		t.Run(string(operation), func(t *testing.T) {
			fixture := partialIdentityFixture{operation: operation}
			semantic := fixture.semantic()
			asset, err := MutationProgram(semantic, fixture.config(semantic))
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition, "{{OPERATION}}", strings.ToUpper(string(operation))).Replace(partialIdentitySQLiteFixture)
			(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
		})
	}
}

func TestDirectEntitySupportRejectsPartialSequenceIdentity(t *testing.T) {
	f := partialIdentityFixture{operation: plan.OperationPost}
	semantic := f.semantic()
	semantic.Root.Entity.Hooks = spec.TypeRef{}
	semantic.Root.Entity.HooksBind = false
	asset, err := EntitySupport(semantic, f.config(semantic))
	if err != nil {
		t.Fatal(err)
	}
	source := partialIdentityTypes + `
func TestStrictIdentity(t *testing.T){
 input:=&Input{Events:[]*Record{{TenantId:0,Name:"new",Has:&Marker{TenantId:true,Name:true}}}}
 captured,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 if err=captured.(*_newEventsHandlerOriginalInput).SyncPresence(input);err==nil||!strings.Contains(err.Error(),"partial original identity"){t.Fatalf("direct entity support accepted partial identity: %v",err)}
}
`
	t.Run("public entity support", func(t *testing.T) {
		(entitySyncFixture{entity: asset, source: source}).run(t)
	})
	t.Run("direct handler", func(t *testing.T) {
		direct, err := Lower(semantic, f.config(semantic))
		if err != nil {
			t.Fatal(err)
		}
		(entitySyncFixture{entity: direct.Entities, products: []*ast.File{direct.File}, source: source}).run(t)
	})
}

func TestMutationIdentityRejectsUnrelatedSelfRoot(t *testing.T) {
	f := partialIdentityFixture{operation: plan.OperationPost}
	semantic := f.semantic()
	semantic.Root.Sequence = nil
	semantic.Root.SelfRelations = []plan.SelfRelationPlan{{FieldPath: plan.FieldPath{"Children"}, Links: []plan.KeyLink{{Parent: semantic.Root.Keys[0], Child: semantic.Root.Keys[0], Conversion: plan.LinkDirect}}}}
	semantic.Root.Entity.Fields = append(semantic.Root.Entity.Fields, plan.EntityField{Name: "Children", Type: spec.TypeRef{Name: "[]*Record"}, Relation: true, Self: true, Writable: true})
	asset, err := MutationProgram(semantic, f.config(semantic))
	if err != nil {
		t.Fatal(err)
	}
	source := strings.NewReplacer("Id,TenantId,Name bool", "Id,TenantId,Name,Children bool", "Name string;Has *Marker", "Name string;Has *Marker;Children []*Record").Replace(partialIdentityTypes) + `
func TestSelfProducerIsNotLocalSequence(t *testing.T){
 input:=&Input{Events:[]*Record{{TenantId:0,Has:&Marker{TenantId:true}}}}
 captured,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 err=captured.(*_newEventsHandlerOriginalInput).SyncPresence(input)
 if err==nil||!strings.Contains(err.Error(),"active captured producer"){t.Fatalf("unrelated root acquired self-link partial-key authority: %v",err)}
}
`
	(entitySyncFixture{entity: asset.Entities, source: source}).run(t)
}

const partialIdentityTypes = `package events
import("context";"strings";"testing")
type Marker struct{Id,TenantId,Name bool}
type Record struct{Id *int64;TenantId int64;Name string;Has *Marker}
type Input struct{Events []*Record}
type Output struct{Data []*Record}
`

const partialIdentitySQLiteFixture = `package events
import (
 "context";"fmt";"reflect";"strings";"testing";"time"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
type Marker struct{Id,TenantId,Name bool}
` + "type Record struct{Id *int64 `sqlx:\"id,primaryKey\"`;TenantId int64 `sqlx:\"tenant_id,primaryKey\"`;Name string `sqlx:\"name\" validate:\"required\"`;Has *Marker `setMarker:\"true\" sqlx:\"-\"`}\n" +
	"type Previous struct{Id *int64 `sqlx:\"id\"`;TenantId int64 `sqlx:\"tenant_id\"`;Name string `sqlx:\"name\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents []*Previous;Mode string}
type Output struct{Data []*Record}
func(i *Input)Init(context.Context)error{if i.Mode=="changed tenant"{for _,row:=range i.Events{row.TenantId=91;row.Has.TenantId=false}};return nil}
` + "type Hooks struct{Input *Input `bind:\"kind=input\"`}\n" + `
var initialized,validated,sequenced,queued int
func(h *Hooks)Init(_ context.Context,row *Record,state handler.EntityState[Record,handler.NoParent])error{
 initialized++
 if state.Previous!=nil{return fmt.Errorf("incomplete key matched Previous")}
 if !state.Original.Has("TenantId"){return fmt.Errorf("original tenant lost")}
 return nil
}
func(h *Hooks)Validate(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{validated++;return nil}
func(h *Hooks)AfterSequence(_ context.Context,row *Record,_ handler.EntityState[Record,handler.NoParent])error{sequenced++;if h.Input.Mode=="changed tenant"{row.TenantId=92;row.Has.TenantId=false};return nil}
func(h *Hooks)AfterQueue(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{queued++;return nil}
func TestPartialProgram(t *testing.T){
 for _,mode:=range []string{"tenant zero","tenant seven","changed tenant","missing tenant","supplied nil","business failure"}{t.Run(mode,func(t *testing.T){
  initialized=0;validated=0;sequenced=0;queued=0
  ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();h:=sqlite.New(t)
  h.DB.SetMaxOpenConns(1);h.DB.SetMaxIdleConns(1)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL,name TEXT,PRIMARY KEY(tenant_id,id))","INSERT INTO records VALUES(0,0,'stored zero'),(7,0,'stored seven'),(5,10,'highest')","CREATE TABLE trace(action TEXT)","CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO trace VALUES('insert'); END","CREATE TRIGGER updated AFTER UPDATE ON records BEGIN INSERT INTO trace VALUES('update'); END");err!=nil{t.Fatal(err)}
  bindings:=[]bindly.BindingSpec{{Path:"CurrentEvents",Name:"CurrentEvents",Location:bindstate.Location{Kind:"view",In:"Current"}},{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}},{Path:"Mode",Name:"Mode",Location:bindstate.Location{Kind:"test",In:"mode"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
  bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
  component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"CurrentEvents",Source:spec.BindSource{Kind:"view",Name:"Current"}}},Views:[]*spec.View{{Name:"Current",Source:&spec.ViewSource{SQL:"SELECT id,tenant_id,name FROM records"}}}}
  dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:inputType,Bindings:bindings});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:projection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
  ref:=spec.RouteRef{Method:"{{OPERATION}}",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
  tenant:=int64(0);if mode=="tenant seven"{tenant=7}
  row:=&Record{TenantId:tenant,Name:"inserted",Has:&Marker{TenantId:true,Name:true}}
  if mode=="missing tenant"{id:=int64(0);row.Id=&id;row.Has.Id=true;row.Has.TenantId=false}
  if mode=="supplied nil"{row.Has.Id=true}
  if mode=="business failure"{row.Name=""}
  definition:={{FACTORY}}().(*{{DEFINITION}})
  _,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{views,values.New("test",map[string]any{"events":[]*Record{row},"mode":mode})}})
  if mode=="changed tenant"&&"{{OPERATION}}"!="PUT"{
   if err==nil||!strings.Contains(err.Error(),"frozen resolved identity")||initialized!=1||validated!=1||sequenced!=1||queued!=0{t.Fatalf("known part retarget not rejected: %v",err)}
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM trace"},[]struct{N int}{{0}});return
  }
  success:="{{OPERATION}}"!="PUT"&&mode!="missing tenant"&&mode!="supplied nil"&&mode!="business failure"
  if success{
   if err!=nil{t.Fatal(err)}
   if row.Id==nil||*row.Id<=10||row.TenantId!=tenant||!row.Has.TenantId{t.Fatalf("supplied tenant or generated ID lost: %+v",row)}
   if initialized!=1||validated!=1||sequenced!=1||queued!=1{t.Fatalf("hook counts=%d/%d/%d/%d",initialized,validated,sequenced,queued)}
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT tenant_id,id,name FROM records WHERE id>10"},[]struct{TenantId,Id int64;Name string}{{tenant,*row.Id,"inserted"}})
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT action FROM trace"},[]struct{Action string}{{"insert"}})
  }else{
   if err==nil{t.Fatal("invalid partial identity or business field accepted")}
   if mode=="supplied nil"&&!strings.Contains(err.Error(),"supplied without a value"){t.Fatalf("wrong supplied-null error: %v",err)}
   if mode!="supplied nil"&&mode!="business failure"&&!strings.Contains(err.Error(),"partial resolved identity"){t.Fatalf("wrong partial-key error: %v",err)}
   if validated!=0||sequenced!=0||queued!=0{t.Fatalf("failure escaped prechecks: %v hooks=%d/%d/%d",err,validated,sequenced,queued)}
   if mode!="missing tenant"&&row.Id!=nil{t.Fatal("identity allocated before failure")}
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM trace"},[]struct{N int}{{0}})
  }
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT tenant_id,id,name FROM records WHERE id<=10 ORDER BY tenant_id"},[]struct{TenantId,Id int64;Name string}{{0,0,"stored zero"},{5,10,"highest"},{7,0,"stored seven"}})
 })}
}
type allocationProbe struct{data *dml.Data;calls int}
func(p *allocationProbe)Allocate(ctx context.Context,table string,value any,field string)error{p.calls++;return p.data.Allocate(ctx,table,value,field)}
type actionBinder struct{data *dml.Data;probe *allocationProbe;input *Input}
func(b actionBinder)Bind(_ context.Context,target any)error{
 a,ok:=target.(*_newEventsHandlerMutationActions);if !ok{return fmt.Errorf("unexpected action binding %T",target)}
 a.DML=b.data;a.Sequencer=b.probe;a.Input=b.input;return nil
}
func(b actionBinder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
func TestActionIdentitySQLite(t *testing.T){
 if "{{OPERATION}}"!="POST"{t.Skip("POST action seam; canonical Program above covers all operations")}
 for _,mode:=range []string{"partial","explicit zero","contradictory prior"}{t.Run(mode,func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL,name TEXT,PRIMARY KEY(tenant_id,id))","INSERT INTO records VALUES(5,10,'existing')");err!=nil{t.Fatal(err)}
  row:=&Record{TenantId:0,Name:"new",Has:&Marker{TenantId:true,Name:true}}
  if mode=="explicit zero"{zero:=int64(0);row.Id=&zero;row.Has.Id=true}
  input:=&Input{Events:[]*Record{row}}
  captured,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
  original:=captured.(*_newEventsHandlerOriginalInput)
  key,complete,insertOnly,err:=(_newEventsHandlerMatchAdapter0{}).Identity(original.Roots[0]);if err!=nil{t.Fatal(err)}
  if key.TenantId!=0||complete!=(mode=="explicit zero")||insertOnly!=(mode!="explicit zero"){t.Fatalf("classification: key=%+v complete=%v insertOnly=%v",key,complete,insertOnly)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,nil);if err!=nil{t.Fatal(err)}
  synced,err:=original.synchronize(input);if err!=nil{t.Fatal(err)};frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
  if mode=="contradictory prior"{zero:=int64(0);frames.Role0[0].State.Previous=&Record{Id:&zero,TenantId:0}}
  data:=dml.NewData(h.DB);if err=data.BeginInvocation();err!=nil{t.Fatal(err)};defer data.Complete(ctx,fmt.Errorf("cleanup"))
  probe:=&allocationProbe{data:data};actions:=&_newEventsHandlerMutationActions{}
  if err=actions.Prepare(ctx,actionBinder{data:data,probe:probe,input:input});err!=nil{t.Fatal(err)}
  if err=data.Start(ctx);err!=nil{t.Fatal(err)}
  err=actions.Sequence(ctx,frames)
  if mode=="contradictory prior"{
   if err==nil||probe.calls!=0||row.Id!=nil{t.Fatalf("contradictory Previous reached allocator: %v calls=%d",err,probe.calls)}
   return
  }
  if err!=nil{t.Fatal(err)}
  wantCalls:=1;if mode=="explicit zero"{wantCalls=0}
  if probe.calls!=wantCalls{t.Fatalf("allocation calls=%d want=%d",probe.calls,wantCalls)}
  if err=actions.Diff(ctx,frames);err!=nil{t.Fatal(err)}
  if len(actions.role0)!=1||actions.role0[0].Action!=handler.WriteInsert{t.Fatal("sequence changed INSERT classification")}
  if err=actions.Reconcile(ctx,frames);err!=nil{t.Fatal(err)}
  if row.TenantId!=0||row.Id==nil||!row.Has.TenantId{t.Fatalf("known zero changed: %+v",row)}
  if mode=="explicit zero"&&*row.Id!=0{t.Fatal("explicit zero ID was allocated")}
  if err=actions.Queue(ctx,frames);err!=nil{t.Fatal(err)};if err=data.Complete(ctx,nil);err!=nil{t.Fatal(err)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT tenant_id,id,name FROM records WHERE tenant_id=0"},[]struct{TenantId,Id int64;Name string}{{0,*row.Id,"new"}})
 })}
}
`
