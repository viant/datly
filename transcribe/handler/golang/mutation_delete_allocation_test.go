package golang

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestDeleteErrorsPrecedeEveryAllocationAndDML(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	root := semantic.Root
	root.Sequence.Field = plan.FieldRef{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "*int64"}}
	root.Write.DeleteMarker = plan.FieldRef{Field: "Remove", Type: spec.TypeRef{Name: "bool"}}
	root.Write.Allowed = append(root.Write.Allowed, plan.ActionDelete)
	root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Remove", Type: spec.TypeRef{Name: "bool"}, DeleteMarker: true}}}
	root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
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
	runEntitySyncFixture(t, semantic, types, deleteAllocationFixture, frames.File, frames.Previous.File, frames.Layout.File, actions.File)
}

const deleteAllocationFixture = `package events
import("context";"testing";"github.com/viant/xdatly/handler")
type Marker struct{Id,Name,Remove bool};type Record struct{Id *int64;Name string;Remove bool;Has *Marker};type Previous struct{Id *int64}
type Input struct{Events []*Record;CurrentEvents []*Previous};type Output struct{Data []*Record}
type projection struct{};func(projection)Has(string)bool{return true};func(projection)RootHolder()string{return ""};func(projection)DirectOutput()bool{return true};func(projection)Fields(int,...handler.ReadStep)(handler.FieldSet,error){return projection{},nil}
type metadata struct{};func(metadata)Projection(string)(handler.ReadProjection,error){return projection{},nil}
type data struct{allocations,writes int};func(d *data)Allocate(context.Context,string,any,string)error{d.allocations++;return nil};func(d *data)Insert(string,any)error{d.writes++;return nil};func(d *data)Update(string,any)error{d.writes++;return nil};func(d *data)Delete(string,any)error{d.writes++;return nil};func(d *data)Execute(string,...any)error{d.writes++;return nil}
type binder struct{data *data;input *Input};func(b binder)Bind(_ context.Context,target any)error{a:=target.(*_newEventsHandlerMutationActions);a.DML=b.data;a.Sequencer=b.data;a.Input=b.input;return nil};func(b binder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
func TestFailBeforeAllocation(t *testing.T){
 for _,unknown:=range []bool{false,true}{
  id:=int64(999);bad:=&Record{Remove:true,Has:&Marker{Remove:true}};if unknown{bad.Id=&id;bad.Has.Id=true}
  first:=&Record{Has:&Marker{}}
  input:=&Input{Events:[]*Record{first,bad}}
  ctx:=context.Background();original,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata{});if err!=nil{t.Fatal(err)}
  synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
  frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
  native:=&data{};actions:=&_newEventsHandlerMutationActions{}
  if err=actions.Prepare(ctx,binder{native,input});err!=nil{t.Fatal(err)}
  if err=actions.Sequence(ctx,frames);err==nil{t.Fatal("unsafe deletion accepted")}
  if native.allocations!=0||native.writes!=0||first.Id!=nil{t.Fatalf("work happened before invalid deletion was rejected: %+v",native)}
 }
}
`
