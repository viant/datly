package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationActionsMixedGraphSQLite(t *testing.T) {
	semantic := recursiveSemanticPlan(plan.OperationPatch)
	root := semantic.Root
	child := root.Relations[0].Child
	child.Relations = nil
	child.Sequence = nil
	root.Sequence.Field = plan.FieldRef{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "*int64"}}
	child.Keys = []plan.KeyPart{{Field: "TenantId", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}}, {Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "int64"}}}
	child.Current.Keys = append([]plan.KeyPart(nil), child.Keys...)
	root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Order"}, MarkerField: "Has", MarkerPointer: true, Keys: root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Items", Type: spec.TypeRef{Name: "[]*Item"}, Relation: true, Writable: true}}}
	child.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Item"}, MarkerField: "Has", MarkerPointer: true, Keys: child.Keys, Fields: []plan.EntityField{{Name: "TenantId", Type: spec.TypeRef{Name: "int64"}, Identity: true, Writable: true}, {Name: "Id", Type: spec.TypeRef{Name: "int64"}, Identity: true, Writable: true}, {Name: "OrderId", Type: spec.TypeRef{Name: "int64"}, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	for _, record := range []*plan.RecordPlan{root, child} {
		for _, field := range record.Entity.Fields {
			if field.Relation {
				continue
			}
			record.Current.Fields = append(record.Current.Fields, plan.CurrentField{Current: plan.FieldRef{Field: field.Name, Type: field.Type}, Entity: plan.FieldRef{Field: field.Name, Type: field.Type}, Conversion: plan.LinkDirect})
		}
	}
	types := []RecordType{{Identity: root.Identity, Path: root.InputPath, Value: "[]*Order", Current: "[]*CurrentOrder"}, {Identity: child.Identity, Path: child.InputPath, Value: "[]*Item", Current: "[]*CurrentItem"}}
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := MutationFrameSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := MutationActionSupport(semantic, config, entities, frames)
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, actionSQLiteFixture, frames.File, frames.Previous.File, frames.Layout.File, actions.File)
}

const actionSQLiteFixture = `package events
import(
 "context";"fmt";"reflect";"testing"
 "github.com/viant/bindly";bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
type OrderHas struct{Id,Name,Items bool};type ItemHas struct{TenantId,Id,OrderId,Name bool}
` + "type Order struct{Id *int64 `sqlx:\"id,primaryKey\"`;Name string `sqlx:\"name\"`;Items []*Item `sqlx:\"-\"`;Has *OrderHas `sqlx:\"-\" setMarker:\"true\"`}\n" +
	"type Item struct{TenantId int64 `sqlx:\"tenant_id,primaryKey\"`;Id int64 `sqlx:\"id,primaryKey\"`;OrderId int64 `sqlx:\"order_id\"`;Name string `sqlx:\"name\"`;Has *ItemHas `sqlx:\"-\" setMarker:\"true\"`}\n" +
	"type CurrentOrder struct{Id *int64 `sqlx:\"id\"`;Name string `sqlx:\"name\"`}\n" +
	"type CurrentItem struct{TenantId int64 `sqlx:\"tenant_id\"`;Id int64 `sqlx:\"id\"`;OrderId int64 `sqlx:\"order_id\"`;Name string `sqlx:\"name\"`}\n" + `
type Input struct{Orders []*Order;CurrentOrders []*CurrentOrder;CurrentItems []*CurrentItem};type Output struct{Data []*Order}
type metadata map[string]handler.ReadProjection
func(m metadata)Projection(name string)(handler.ReadProjection,error){p,ok:=m[name];if !ok{return nil,fmt.Errorf("missing read metadata %s",name)};return p,nil}
type binder struct{data *dml.Data;input *Input}
func(b binder)Bind(ctx context.Context,target any)error{a:=target.(*_newEventsHandlerMutationActions);a.DML=b.data;a.Sequencer=b.data;a.Input=b.input;return nil}
func(b binder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
func ptr(value int64)*int64{return &value}
func TestMixedSQL(t *testing.T){
 for _,explicitZero:=range []bool{false,true}{t.Run(fmt.Sprint("explicitZero=",explicitZero),func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,
   "CREATE TABLE ORDERS(id INTEGER PRIMARY KEY,name TEXT)",
   "CREATE TABLE ITEMS(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL,order_id INTEGER NOT NULL,name TEXT,PRIMARY KEY(tenant_id,id),FOREIGN KEY(order_id) REFERENCES ORDERS(id))",
   "CREATE TABLE trace(value TEXT)",
   "CREATE TRIGGER order_update AFTER UPDATE ON ORDERS BEGIN INSERT INTO trace VALUES('u:order:'||NEW.id); END",
   "CREATE TRIGGER order_insert AFTER INSERT ON ORDERS BEGIN INSERT INTO trace VALUES('i:order:'||NEW.id); END",
   "CREATE TRIGGER item_update AFTER UPDATE ON ITEMS BEGIN INSERT INTO trace VALUES('u:item:'||NEW.tenant_id||':'||NEW.id); END",
   "CREATE TRIGGER item_insert AFTER INSERT ON ITEMS BEGIN INSERT INTO trace VALUES('i:item:'||NEW.tenant_id||':'||NEW.id); END",
   "INSERT INTO ORDERS VALUES(0,'old'),(5,'untouched')",
   "INSERT INTO ITEMS VALUES(1,5,0,'old child'),(2,9,5,'untouched child')",
   "DELETE FROM trace");err!=nil{t.Fatal(err)}
  existing:=&Order{Id:ptr(0),Name:"edited",Has:&OrderHas{Id:true,Name:true},Items:[]*Item{{TenantId:1,Id:5,Name:"edited child",Has:&ItemHas{TenantId:true,Id:true,Name:true}}}}
  created:=&Order{Name:"new",Has:&OrderHas{Name:true},Items:[]*Item{{TenantId:2,Id:5,Name:"new child",Has:&ItemHas{TenantId:true,Id:true,Name:true}}}}
  if explicitZero{created.Items=append(created.Items,&Item{TenantId:2,Id:0,Name:"explicit zero",Has:&ItemHas{TenantId:true,Id:true,Name:true}})}
  input:=&Input{Orders:[]*Order{existing,created}}
  bindings:=[]bindly.BindingSpec{{Path:"CurrentOrders",Name:"CurrentOrders",Location:bindstate.Location{Kind:"view",In:"Orders"}},{Path:"CurrentItems",Name:"CurrentItems",Location:bindstate.Location{Kind:"view",In:"Items"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};typ:=reflect.TypeOf(Input{});boundPlan,err:=seed.CompilePlan(typ,bindings...);if err!=nil{t.Fatal(err)};inputProjection,err:=boundPlan.Projection();if err!=nil{t.Fatal(err)}
  allowNulls:=true;component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"CurrentOrders",Source:spec.BindSource{Kind:"view",Name:"Orders"}},{Name:"CurrentItems",Source:spec.BindSource{Kind:"view",Name:"Items"}}},Views:[]*spec.View{{Name:"Orders",AllowNulls:&allowNulls,Source:&spec.ViewSource{SQL:"SELECT id,name FROM ORDERS ORDER BY id"}},{Name:"Items",Source:&spec.ViewSource{SQL:"SELECT tenant_id,id,order_id,name FROM ITEMS ORDER BY tenant_id,id"}}}}
  dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:typ,Bindings:bindings});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:inputProjection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)};injector,err:=seed.ForScope(views);if err!=nil{t.Fatal(err)}
  evidence:=metadata{};if err=injector.Bind(ctx,input,bindly.WithPlan(boundPlan),bindly.WithBindingObserver(func(_ context.Context,event bindly.BindingEvent)error{p,ok:=event.Metadata.(handler.ReadProjection);if !ok{return fmt.Errorf("actual read evidence missing")};evidence[event.Path]=p;return nil}));err!=nil{t.Fatal(err)}
  original,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)};database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,evidence);if err!=nil{t.Fatal(err)}
  // Input Init cannot redefine the update identity or corrupt frozen Current.
  *existing.Id=77;existing.Items[0].Id=66;*input.CurrentOrders[0].Id=88;input.CurrentItems[0].Id=99
  sync,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)};frames,err:=database.Build(input,sync);if err!=nil{t.Fatal(err)}
  native:=dml.NewData(h.DB);if err=native.BeginInvocation();err!=nil{t.Fatal(err)};defer native.Complete(ctx,fmt.Errorf("test cleanup"))
  actions:=&_newEventsHandlerMutationActions{};if err=actions.Prepare(ctx,binder{native,input});err!=nil{t.Fatal(err)}
  if !actions.RequiresTransaction(frames){t.Fatal("missing transaction requirement")};if err=native.Start(ctx);err!=nil{t.Fatal(err)}
  if err=actions.Sequence(ctx,frames);err!=nil{t.Fatal(err)};if err=actions.Diff(ctx,frames);err!=nil{t.Fatal(err)};if err=actions.Reconcile(ctx,frames);err!=nil{t.Fatal(err)}
  if *existing.Id!=0||existing.Items[0].Id!=5||existing.Items[0].OrderId!=0||created.Id==nil||*created.Id<=5||created.Items[0].OrderId!=*created.Id{t.Fatalf("identity/link/output reconciliation failed: existing %+v new %+v",existing,created)}
  if err=actions.Queue(ctx,frames);err!=nil{t.Fatal(err)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM ITEMS"},[]struct{N int}{ {2} })
  if err=native.Complete(ctx,nil);err!=nil{t.Fatal(err)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM ORDERS ORDER BY id"},[]CurrentOrder{{Id:ptr(0),Name:"edited"},{Id:ptr(5),Name:"untouched"},{Id:ptr(*created.Id),Name:"new"}})
  wanted:=[]CurrentItem{{TenantId:1,Id:5,OrderId:0,Name:"edited child"}}
  trace:=[]struct{Value string}{{"u:order:0"},{"u:item:1:5"},{fmt.Sprint("i:order:",*created.Id)},{"i:item:2:5"}}
  if explicitZero{wanted=append(wanted,CurrentItem{TenantId:2,Id:0,OrderId:*created.Id,Name:"explicit zero"});trace=append(trace,struct{Value string}{"i:item:2:0"})}
  wanted=append(wanted,CurrentItem{TenantId:2,Id:5,OrderId:*created.Id,Name:"new child"},CurrentItem{TenantId:2,Id:9,OrderId:5,Name:"untouched child"})
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT tenant_id,id,order_id,name FROM ITEMS ORDER BY tenant_id,id"},wanted)
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT value FROM trace ORDER BY rowid"},trace)
 })}
}
`
