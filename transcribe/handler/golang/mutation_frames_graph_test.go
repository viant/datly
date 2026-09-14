package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationFramesNestedAndSelfContexts(t *testing.T) {
	semantic := recursiveSemanticPlan(plan.OperationPost)
	root := semantic.Root
	child := root.Relations[0].Child
	leaf := child.Relations[0].Child
	names := []string{"Order", "Item", "Detail"}
	holders := []string{"Items", "Details"}
	var types []RecordType
	for i, record := range []*plan.RecordPlan{root, child, leaf} {
		record.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: names[i]}, MarkerField: "Has", MarkerPointer: true, Keys: record.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}}}
		if i < 2 {
			record.Entity.Fields = append(record.Entity.Fields, plan.EntityField{Name: holders[i], Type: spec.TypeRef{Name: "[]*" + names[i+1]}, Relation: true, Writable: true})
		}
		if i == 1 {
			record.Entity.Fields = append(record.Entity.Fields, plan.EntityField{Name: "Children", Type: spec.TypeRef{Name: "[]*Item"}, Relation: true, Self: true, Writable: true})
		}
		types = append(types, RecordType{Identity: record.Identity, Path: record.InputPath, Value: "[]*" + names[i]})
	}
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := MutationFrameSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, `package events
import("context";"strings";"testing")
type Marker struct{Id,Items,Details,Children bool}
type Order struct{Id *int64;Items []*Item;Has *Marker}
type Item struct{Id *int64;OrderId int64;Details []*Detail;Children []*Item;Has *Marker}
type Detail struct{Id *int64;ItemId int64;Has *Marker}
type Input struct{Orders []*Order};type Output struct{Data []*Order}
func TestFrameGraph(t *testing.T){
 for _,mode:=range []string{"nested self","shared parent ambiguity","cycle context ambiguity"}{t.Run(mode,func(t *testing.T){
  detail:=&Detail{Has:&Marker{}};item:=&Item{Has:&Marker{},Details:[]*Detail{detail}};self:=&Item{Has:&Marker{}}
  item.Children=[]*Item{self};order:=&Order{Has:&Marker{},Items:[]*Item{item}};input:=&Input{Orders:[]*Order{order}}
  if mode=="shared parent ambiguity"{input.Orders=append(input.Orders,&Order{Has:&Marker{},Items:[]*Item{item}})}
  if mode=="cycle context ambiguity"{self.Children=[]*Item{item}}
  snapshot,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
  synced,err:=snapshot.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
  db,err:=_newEventsHandlerDatabaseSnapshotCapture(input,nil);if err!=nil{t.Fatal(err)}
  frames,err:=db.Build(input,synced)
  if mode!="nested self"{if err==nil||!strings.Contains(err.Error(),"ambiguous parent")||frames!=nil{t.Fatalf("got %v err%v",frames,err)};return}
  if err!=nil{t.Fatal(err)}
  if len(frames.Role0)!=1||len(frames.Role1)!=2||len(frames.Role2)!=1{t.Fatalf("frame counts %+v",frames)}
  if frames.Role0[0].State.Parent!=nil||frames.Role1[0].State.Parent!=order||frames.Role1[0].State.SelfParent!=nil||frames.Role1[1].State.Parent!=order||frames.Role1[1].State.SelfParent!=item||frames.Role2[0].State.Parent!=item{t.Fatal("typed parent context lost")}
  if frames.Role1[0].Entity!=item||frames.Role1[1].Entity!=self||frames.Role2[0].Entity!=detail{t.Fatal("traversal order lost")}
 })}
}
func TestFrameVerification(t *testing.T){
 for _,mode:=range []string{"unchanged","scalar change","added nil slot","removed root","reordered roots","added child","removed child","reparented self","duplicate child","replaced child","new cycle","corrupt order","truncated order"}{t.Run(mode,func(t *testing.T){
  detail:=&Detail{Has:&Marker{}};self:=&Item{Has:&Marker{}}
  item:=&Item{Has:&Marker{},Details:[]*Detail{detail},Children:[]*Item{self}}
  first:=&Order{Has:&Marker{},Items:[]*Item{item}};second:=&Order{Has:&Marker{}}
  input:=&Input{Orders:[]*Order{first,second}}
  original,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
  synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,nil);if err!=nil{t.Fatal(err)}
  frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
  wantOrder:=[][2]int{{0,0},{1,0},{2,0},{1,1},{0,1}}
  if len(frames.Order)!=len(wantOrder){t.Fatalf("order length=%d",len(frames.Order))}
  for index,want:=range wantOrder{if frames.Order[index].Role!=want[0]||frames.Order[index].Index!=want[1]{t.Fatalf("visit %d=%+v",index,frames.Order[index])}}
  if err:=frames.Verify(input);err!=nil{t.Fatalf("initial graph: %v",err)}
  switch mode{
  case "scalar change":id:=int64(7);item.Id=&id;item.OrderId=99;item.Has.Id=true
  case "added nil slot":first.Items=append(first.Items,nil)
  case "removed root":input.Orders=input.Orders[:1]
  case "reordered roots":input.Orders[0],input.Orders[1]=input.Orders[1],input.Orders[0]
  case "added child":first.Items=append(first.Items,&Item{})
  case "removed child":item.Details=nil
  case "reparented self":item.Children=nil;second.Items=[]*Item{self}
  case "duplicate child":first.Items=append(first.Items,item)
  case "replaced child":first.Items[0]=&Item{}
  case "new cycle":self.Children=[]*Item{item}
  case "corrupt order":frames.Order[0].Role=99
  case "truncated order":frames.Order=frames.Order[:1]
  }
  err=frames.Verify(input)
  allowed:=mode=="unchanged"||mode=="scalar change"||mode=="added nil slot"
  if (err==nil)!=allowed{t.Fatalf("verification=%v",err)}
  if err:=frames.Verify(nil);err==nil{t.Fatal("nil input accepted")}
 })}
}
`, frames.File, frames.Previous.File, frames.Layout.File)
}
