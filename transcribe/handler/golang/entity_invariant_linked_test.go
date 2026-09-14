package golang

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLinkedInvariantBackfillUsesCanonicalPathsAtomically(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Presence", MarkerPointer: true, Fields: []plan.EntityField{
		{Name: "Start", Path: plan.FieldPath{"Values", "Start"}, Type: spec.TypeRef{Name: "*int"}, Writable: true},
		{Name: "End", Path: plan.FieldPath{"Values", "End"}, Type: spec.TypeRef{Name: "*int"}, Writable: true},
		{Name: "Note", Path: plan.FieldPath{"Values", "Note"}, Type: spec.TypeRef{Name: "string"}, Writable: true},
	}, Invariants: []plan.InvariantGroup{{Name: "Window", Fields: []string{"Start", "End", "Note"}}}}
	// This fixture exercises backfill independently of identity capture.
	semantic.Root.Keys = nil
	semantic.Root.Sequence = nil
	types := rootRecordTypes(semantic, "[]*Record", "")
	asset, err := EntitySupport(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err != nil {
		t.Fatal(err)
	}
	if len(asset.Invariants) != 1 || asset.Invariants[0].BackfillFunction == "" {
		t.Fatal("linked invariant helper absent")
	}
	for _, method := range asset.Methods {
		if strings.Contains(method.Name, "Window") {
			t.Fatal("declared method on linked entity")
		}
	}
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	layout, err := MutationFrames(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	phase, err := MutationInvariantSupport(semantic, config, asset, layout)
	if err != nil {
		t.Fatal(err)
	}
	if phase == nil {
		t.Fatal("invariant phase missing")
	}
	source := strings.NewReplacer("{{BACKFILL}}", asset.Invariants[0].BackfillFunction, "{{PHASE}}", phase.Function, "{{FRAMES}}", layout.TypeName, "{{FRAME}}", layout.Roles[0].FrameType, "{{ROLE}}", layout.Roles[0].Field).Replace(linkedInvariantFixture + linkedInvariantPhaseFixture)
	runEntitySyncFixture(t, semantic, types, source, layout.File, phase.File)
}

const linkedInvariantFixture = `package events
import("testing";"strings";"context";"errors";"github.com/viant/xdatly/handler")
type Flag bool
type Flags struct{Start,End,Note Flag}
type Marker struct{*Flags;Version int;callback func()}
type Values struct{Start,End *int;Note string}
type Record struct{*Values;Presence *Marker;service func()}
type Input struct{Events []*Record}
type Output struct{Data []*Record}
type fields map[string]bool
func(f fields)Has(name string)bool{return f[name]}
type nilFields struct{}
func(f *nilFields)Has(name string)bool{panic("typed nil field set invoked")}
func ptr(v int)*int{return &v}
func TestLinkedBackfill(t *testing.T){
 retained:=&Values{Note:"existing"}
 current:=&Record{Values:retained,Presence:&Marker{Flags:&Flags{Start:true}}}
 if err:={{BACKFILL}}(current,&Record{Values:&Values{End:ptr(7),Note:"loaded"}},fields{"End":true,"Note":true});err!=nil{t.Fatal(err)}
 if current.Values!=retained{t.Fatal("backfill relocated existing holder and invalidated entity frame addresses")}
 for _,tc:=range []struct{name string;loaded fields;inactive bool;fail string}{
  {"explicit nil and missing embedding",fields{"End":true,"Note":true},false,""},
  {"unknown sibling atomic",fields{"End":true},false,"Note was not loaded"},
  {"unaffected group",fields{},true,""},
 }{t.Run(tc.name,func(t *testing.T){
  calls:=0;callback:=func(){calls++}
  marker:=&Marker{Flags:&Flags{Start:Flag(!tc.inactive)},Version:12,callback:callback}
  entity:=&Record{Presence:marker,service:callback}
  previous:=&Record{Values:&Values{Start:ptr(3),End:ptr(7),Note:"retained"}}
  err:={{BACKFILL}}(entity,previous,tc.loaded)
  if tc.fail!=""{if err==nil||!strings.Contains(err.Error(),tc.fail){t.Fatalf("error=%v",err)};if entity.Values!=nil{t.Fatal("failed group changed target embedding")};return}
  if err!=nil{t.Fatal(err)}
  if tc.inactive{if entity.Values!=nil{t.Fatal("inactive group hydrated")};return}
  if entity.Start!=nil||entity.End==nil||*entity.End!=7||entity.Note!="retained"{t.Fatalf("wrong values %+v",entity.Values)}
  *previous.End=9
  if *entity.End!=7{t.Fatal("backfill aliases previous")}
  if entity.Presence!=marker||marker.End||marker.Note||!marker.Start||marker.Version!=12{t.Fatal("backfill changed markers")}
  entity.service();marker.callback();if calls!=2{t.Fatal("unrelated services lost")}
 })}
 entity:=&Record{Values:&Values{Start:ptr(0),End:nil,Note:""},Presence:&Marker{Flags:&Flags{Start:true,End:true,Note:true}}}
 if err:={{BACKFILL}}(entity,&Record{Values:&Values{Start:ptr(8),End:ptr(9),Note:"other"}},fields{});err!=nil{t.Fatal(err)}
 if *entity.Start!=0||entity.End!=nil||entity.Note!=""{t.Fatal("explicit zero/null/empty overwritten")}
 entity=&Record{Presence:&Marker{Flags:&Flags{Start:true}}}
 if err:={{BACKFILL}}(entity,&Record{Values:&Values{}},(*nilFields)(nil));err==nil||!strings.Contains(err.Error(),"typed nil"){t.Fatalf("error=%v",err)}
 if entity.Values!=nil{t.Fatal("typed nil evidence mutated entity")}
 if err:={{BACKFILL}}(nil,nil,nil);err!=nil{t.Fatal(err)}
}
`

const linkedInvariantPhaseFixture = `
func TestInvariantPhase(t *testing.T){
 entity:=&Record{Presence:&Marker{Flags:&Flags{Start:true}}}
 previous:=&Record{Values:&Values{End:ptr(7),Note:"known"}}
 frame:=&{{FRAME}}{Entity:entity,State:handler.EntityState[Record,handler.NoParent]{Previous:previous}}
 frames:=&{{FRAMES}}{ {{ROLE}}:[]*{{FRAME}}{frame} }
 if err:={{PHASE}}(context.Background(),frames);err==nil{t.Fatal("phase accepted missing previous field evidence")}
 if entity.Values!=nil{t.Fatal("failed phase mutated entity")}
 frame.State.PreviousFields=fields{"End":true,"Note":true}
 ctx,cancel:=context.WithCancel(context.Background());cancel()
 if err:={{PHASE}}(ctx,frames);!errors.Is(err,context.Canceled){t.Fatalf("cancellation=%v",err)}
 if entity.Values!=nil{t.Fatal("cancelled phase mutated entity")}
 if err:={{PHASE}}(context.Background(),frames);err!=nil{t.Fatal(err)}
 if entity.End==nil||*entity.End!=7||entity.Note!="known"||entity.Presence.End||entity.Presence.Note{t.Fatal("phase did not backfill without marking")}
 if err:={{PHASE}}(context.Background(),nil);err==nil{t.Fatal("nil frames accepted")}
 frames.{{ROLE}}=[]*{{FRAME}}{nil}
 if err:={{PHASE}}(context.Background(),frames);err==nil{t.Fatal("nil frame accepted")}
}
`
