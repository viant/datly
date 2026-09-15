package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestDeleteReservationPolicy(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		name := "captured"
		if resolved {
			name = "resolved"
		}
		t.Run(name, func(t *testing.T) {
			semantic := rootSemanticPlan(plan.OperationPatch, false)
			root := semantic.Root
			root.Sequence.Field = plan.FieldRef{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "*int64"}}
			root.Write.DeleteMarker = plan.FieldRef{Field: "Remove", Type: spec.TypeRef{Name: "bool"}}
			root.Write.Allowed = append(root.Write.Allowed, plan.ActionDelete)
			root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Remove", Type: spec.TypeRef{Name: "bool"}, DeleteMarker: true}}}
			root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}}
			types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
			config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
			var entities *EntityAsset
			var err error
			if resolved {
				entities, err = (&mutationIdentityPolicy{}).entities(semantic, config)
			} else {
				entities, err = EntitySupport(semantic, config)
			}
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
			// Reuse the existing typed capture, read-evidence and capability fixture.
			source, _, _ := strings.Cut(deleteAllocationFixture, "func TestFailBeforeAllocation")
			source = strings.NewReplacer(
				`"context";`, `"context";"fmt";"reflect";`,
				`type data struct{allocations,writes int}`, `type data struct{allocations,writes,reserves int;reserved,deleted []int64;rejectReserve bool}`,
				`d.allocations++;return nil`, `d.allocations++;for _,row:=range value.([]*Record){v:=int64(100);row.Id=&v};return nil`,
				`Allocate(context.Context,string,any,string)`, `Allocate(_ context.Context,_ string,value any,_ string)`,
				`func(d *data)Delete(string,any)error{d.writes++;return nil}`, `func(d *data)Delete(_ string,value any)error{d.writes++;d.deleted=append(d.deleted,*value.(*Record).Id);return nil}`,
			).Replace(source) + deleteReservationCases
			binding := ""
			if resolved {
				binding = `if err=database.bindProducers(original.(*_newEventsHandlerOriginalInput));err!=nil{t.Fatal(err)}`
			}
			source = strings.ReplaceAll(source, "BIND_PRODUCERS", binding)
			(entitySyncFixture{entity: entities, products: []*ast.File{frames.File, frames.Previous.File, frames.Layout.File, actions.File}, source: source}).run(t)
		})
	}
}

const deleteReservationCases = `
func(d *data)Reserve(_ context.Context,table string,value any,selector string)error{
 d.reserves++
 if d.rejectReserve{return fmt.Errorf("delete-only input touched Reserve")}
 if table!="EVENTS"||selector!="Id"||d.allocations!=0{return fmt.Errorf("reservation authority or order changed")}
 for _,row:=range value.([]*Record){
  if row.Id==nil||*row.Id==7{return fmt.Errorf("delete entered reservations")}
  d.reserved=append(d.reserved,*row.Id)
  *row.Id=999 // Optional implementations receive detached payloads.
 }
 return nil
}
func ptr(v int64)*int64{return &v}
func TestReservations(t *testing.T){
 for _,mode:=range []string{"delete only","delete and supplied writes","supplied writes only","delete and allocation","unknown delete","missing delete"}{t.Run(mode,func(t *testing.T){
  input:=&Input{CurrentEvents:[]*Previous{{Id:ptr(7)},{Id:ptr(0)},{Id:ptr(8)}}}
  if mode!="supplied writes only"{input.Events=append(input.Events,&Record{Id:ptr(7),Remove:true,Has:&Marker{Id:true,Remove:true}})}
  if mode!="delete only"{input.Events=append(input.Events,
   &Record{Id:ptr(0),Has:&Marker{Id:true}}, // Supplied zero UPDATE identity.
   &Record{Id:ptr(9),Has:&Marker{Id:true}}, // Supplied INSERT identity.
   &Record{Id:ptr(8),Has:&Marker{Id:true,Remove:true}}) // Supplied UPDATE identity.
  }
  if mode=="delete and allocation"{input.Events=append(input.Events,&Record{Has:&Marker{}})}
  invalid:=mode=="unknown delete"||mode=="missing delete"
  if invalid {
   bad:=input.Events[0];input.Events=input.Events[1:]
   if mode=="unknown delete"{bad.Id=ptr(777)}else{bad.Id=nil;bad.Has.Id=false}
   // Put an allocation candidate before the invalid deletion.
   input.Events=append(input.Events,&Record{Has:&Marker{}},bad)
  }
  ctx:=context.Background();original,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata{});if err!=nil{t.Fatal(err)}
  BIND_PRODUCERS
  synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
  frames,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
  native:=&data{rejectReserve:mode=="delete only"};actions:=&_newEventsHandlerMutationActions{}
  if err=actions.Prepare(ctx,binder{native,input});err!=nil{t.Fatal(err)}
  err=actions.Sequence(ctx,frames)
  if invalid {if err==nil||native.allocations!=0||native.reserves!=0||native.writes!=0{t.Fatalf("invalid delete escaped captured identity checks: %v %+v",err,native)};return}
  if err!=nil{t.Fatal(err)}
  allocations:=0;if mode=="delete and allocation"{allocations=1}
  if native.allocations!=allocations||native.writes!=0{t.Fatalf("unexpected sequencer/DML activity: %+v",native)}
  if mode=="delete only"{if native.reserves!=0{t.Fatal("delete requires Reserve")}}else{
   if native.reserves!=1||!reflect.DeepEqual(native.reserved,[]int64{0,9,8}){t.Fatalf("supplied insert/update reservations lost without local candidates: %+v",native)}
  }
  for _,row:=range input.Events{if row.Id!=nil&&*row.Id==999{t.Fatal("Reserve changed working identity")}}
  if err=actions.Diff(ctx,frames);err!=nil{t.Fatal(err)}
  if err=actions.Reconcile(ctx,frames);err!=nil{t.Fatal(err)}
  if err=actions.Queue(ctx,frames);err!=nil{t.Fatal(err)}
  if mode!="supplied writes only"&&!reflect.DeepEqual(native.deleted,[]int64{7}){t.Fatalf("valid delete did not reach DML: %+v",native)}
 })}
}
`
