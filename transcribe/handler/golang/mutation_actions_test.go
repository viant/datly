package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationActionsSequenceAndOriginalDiff(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Sequence.Field = plan.FieldRef{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "*int64"}}
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	semantic.Root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
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
	runEntitySyncFixture(t, semantic, types, `package events
import("context";"fmt";"testing";"github.com/viant/xdatly/handler")
type Marker struct{Id,Name bool};type Record struct{Id *int64;Name string;Has *Marker};type Previous struct{Id *int64;Name string}
type Input struct{Events []*Record;CurrentEvents []*Previous};type Output struct{Data []*Record}
type fields struct{};func(fields)Has(name string)bool{return name=="Id"||name=="Name"}
type projection struct{};func(projection)RootHolder()string{return ""};func(projection)DirectOutput()bool{return true};func(projection)Fields(int,...handler.ReadStep)(handler.FieldSet,error){return fields{},nil}
type metadata struct{};func(metadata)Projection(string)(handler.ReadProjection,error){return projection{},nil}
type data struct{candidates []*Record;calls int;inserts,updates []*Record;order []string}
func(d *data)Allocate(ctx context.Context,table string,value any,selector string)error{if table!="EVENTS"||selector!="Id"{return fmt.Errorf("bad sequence authority")};d.calls++;d.candidates=value.([]*Record);for i,r:=range d.candidates{if r.Id==nil||*r.Id==0{v:=int64(100+i);r.Id=&v}};return nil}
func(d *data)Insert(table string,value any)error{d.inserts=append(d.inserts,value.(*Record));d.order=append(d.order,"insert");return nil};func(d *data)Update(table string,value any)error{d.updates=append(d.updates,value.(*Record));d.order=append(d.order,"update");return nil};func(*data)Delete(string,any)error{return nil};func(*data)Execute(string,...any)error{return nil}
type binder struct{data *data;input *Input};func(b binder)Bind(ctx context.Context,target any)error{a:=target.(*_newEventsHandlerMutationActions);a.DML=b.data;a.Sequencer=b.data;a.Input=b.input;return nil};func(b binder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
type reservingData struct{*data;reserved []int64}
func(d *reservingData)Reserve(_ context.Context,table string,value any,selector string)error{if d.calls!=0||table!="EVENTS"||selector!="Id"{return fmt.Errorf("reservation did not precede allocation")};for _,row:=range value.([]*Record){d.reserved=append(d.reserved,*row.Id);*row.Id=999};return nil}
type reservationBinder struct{binder;reserved *reservingData}
func(b reservationBinder)Bind(ctx context.Context,target any)error{if err:=b.binder.Bind(ctx,target);err!=nil{return err};target.(*_newEventsHandlerMutationActions).Sequencer=b.reserved;return nil}
func ptr(v int64)*int64{return &v}
func TestActions(t *testing.T){
 for _,reserve:=range []bool{false,true}{t.Run(fmt.Sprint(reserve),func(t *testing.T){
 input:=&Input{Events:[]*Record{{Id:ptr(0),Has:&Marker{Id:true}},{Id:ptr(8),Has:&Marker{Id:true}},{Has:&Marker{}},{Has:&Marker{}}},CurrentEvents:[]*Previous{{Id:ptr(0),Name:"existing"}}}
 original,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata{});if err!=nil{t.Fatal(err)}
 *input.Events[0].Id=77;input.Events[3].Id=ptr(55);input.Events[3].Has.Id=true
 synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
 frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
 native:=&data{};actions:=&_newEventsHandlerMutationActions{}
 if !actions.RequiresTransaction(frames)||actions.RequiresTransaction(nil){t.Fatal("transaction requirement")}
 reserved:=&reservingData{data:native};var binding handler.Binder=binder{native,input};if reserve{binding=reservationBinder{binder{native,input},reserved}}
 if err=actions.Prepare(context.Background(),binding);err!=nil{t.Fatal(err)}
 if err=actions.Sequence(context.Background(),frames);err!=nil{t.Fatal(err)}
 if reserve&&(len(reserved.reserved)!=2||reserved.reserved[0]!=0||reserved.reserved[1]!=8){t.Fatalf("reservation lost original keys: %v",reserved.reserved)}
 if native.calls!=1||len(native.candidates)!=2||native.candidates[0]!=input.Events[2]||native.candidates[1]!=input.Events[3]||*input.Events[0].Id!=77||*input.Events[1].Id!=8||*input.Events[2].Id!=100||*input.Events[3].Id!=55{t.Fatal("original identity sequence filtering")}
 if err=actions.Diff(context.Background(),frames);err!=nil{t.Fatal(err)}
 if len(actions.role0)!=4||actions.role0[0].Action!=handler.WriteUpdate||actions.role0[0].OriginalKey.Id!=0||!actions.role0[0].OriginalAssigned{t.Fatal("original zero identity update lost")}
 for _,i:=range []int{1,2,3}{if actions.role0[i].Action!=handler.WriteInsert{t.Fatal("missing/new identity was treated as update")}}
 if actions.role0[2].OriginalAssigned||actions.role0[3].OriginalAssigned{t.Fatal("post-capture flags contaminated identity")}
 if err=actions.Reconcile(context.Background(),frames);err!=nil{t.Fatal(err)}
 if *input.Events[0].Id!=0||!input.Events[2].Has.Id||frames.Role0[2].State.Original.Has("Id"){t.Fatal("reconciled output or immutable identity mask lost")}
 if err=actions.Queue(context.Background(),frames);err!=nil{t.Fatal(err)}
 if len(native.updates)!=1||len(native.inserts)!=3||*native.updates[0].Id!=0||native.updates[0].Has.Id||native.order[0]!="update"{t.Fatal("typed queued policy or identity SET exclusion")}
 input.Events[2].Name="changed after queue";*input.Events[2].Id=200
 if *native.inserts[1].Id!=100||native.inserts[1].Name!=""{t.Fatal("queued payload aliases working values")}
 if err=actions.Queue(context.Background(),frames);err==nil{t.Fatal("queue repeated")}
 })}
}
func TestActionsVerifyBeforeAllocation(t *testing.T){
 for _,mode:=range []string{"append","reorder","cancel"}{t.Run(mode,func(t *testing.T){
  input:=&Input{Events:[]*Record{{Has:&Marker{}},{Has:&Marker{}}}}
  original,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata{});if err!=nil{t.Fatal(err)}
  synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
  frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
  native:=&data{};actions:=&_newEventsHandlerMutationActions{};ctx,cancel:=context.WithCancel(context.Background());defer cancel()
  if err=actions.Prepare(ctx,binder{native,input});err!=nil{t.Fatal(err)}
  switch mode{case "append":input.Events=append(input.Events,&Record{Has:&Marker{}});case "reorder":input.Events[0],input.Events[1]=input.Events[1],input.Events[0];case "cancel":cancel()}
  if err=actions.Sequence(ctx,frames);err==nil||native.calls!=0{t.Fatalf("phase escaped verification: %v calls%d",err,native.calls)}
 })}
}
`, frames.File, frames.Previous.File, frames.Layout.File, actions.File)
}
