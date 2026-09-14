package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationPreviousProjectionAndIdentity(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, true)
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "TenantId", Type: spec.TypeRef{Name: "int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
		{Name: "Values", Type: spec.TypeRef{Name: "[]int"}, Writable: true},
	}}
	semantic.Root.Current.Fields = []plan.CurrentField{
		{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect},
		{Current: plan.FieldRef{Field: "TenantId", Type: spec.TypeRef{Name: "int64"}}, Entity: plan.FieldRef{Field: "TenantId", Type: spec.TypeRef{Name: "int64"}}, Conversion: plan.LinkDirect},
		{Current: plan.FieldRef{Field: "LoadedName", Type: spec.TypeRef{Name: "*string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDereference},
		{Current: plan.FieldRef{Field: "LoadedValues", Type: spec.TypeRef{Name: "[]int"}}, Entity: plan.FieldRef{Field: "Values", Type: spec.TypeRef{Name: "[]int"}}, Conversion: plan.LinkDirect},
	}
	semantic.Root.Current.Self = []plan.FieldRef{{Field: "Children", Type: spec.TypeRef{Name: "[]*Previous"}}}
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
	runEntitySyncFixture(t, semantic, types, `package events
import("testing";"fmt";"strings";"context";"github.com/viant/xdatly/handler")
type Marker struct{Id,TenantId,Name,Values bool}
type Record struct{Id *int64;TenantId int64;Name string;Values []int;Has *Marker}
type Previous struct{Id *int64;TenantId int64;LoadedName *string;LoadedValues []int;Children []*Previous}
type Input struct{Events []*Record;CurrentEvents []*Previous}
type Output struct{Data []*Record}
type fields map[string]bool
func(f fields)Has(name string)bool{return f[name]}
type projection struct{loaded fields;fail bool;steps *[]string;conflict bool}
func(projection)RootHolder()string{return ""};func(projection)DirectOutput()bool{return true}
func(p projection)Fields(root int,steps ...handler.ReadStep)(handler.FieldSet,error){
 if p.fail{return nil,fmt.Errorf("missing provenance")};if p.steps!=nil{*p.steps=append(*p.steps,fmt.Sprint(root,steps))};if p.conflict&&root==1&&len(steps)>0{return fields{"Id":true,"TenantId":true},nil};return p.loaded,nil
}
func pointer[T any](v T)*T{return &v}
func TestPrevious(t *testing.T){
 full:=fields{"Id":true,"TenantId":true,"LoadedName":true,"LoadedValues":true}
 for _,tc:=range []struct{name string;rows []*Previous;loaded fields;want string}{
  {"missing-key",[]*Previous{{Id:pointer(int64(0))}},fields{"TenantId":true},"was not loaded"},
  {"null-key",[]*Previous{{}},full,"is null"},
  {"duplicate-zero",[]*Previous{{Id:pointer(int64(0)),LoadedName:pointer("")},{Id:pointer(int64(0)),LoadedName:pointer("")}},full,"duplicate"},
  {"null-scalar",[]*Previous{{Id:pointer(int64(0))}},full,"loaded null"},
 }{t.Run(tc.name,func(t *testing.T){got,err:=_newEventsHandlerPrevious0Capture(tc.rows,projection{loaded:tc.loaded});if err==nil||!strings.Contains(err.Error(),tc.want)||got!=nil{t.Fatalf("got=%v err=%v",got,err)}})}
 original:=&Previous{Id:pointer(int64(0)),TenantId:2,LoadedName:pointer("retained"),LoadedValues:[]int{3}}
 child:=&Previous{Id:pointer(int64(1)),TenantId:2,LoadedName:pointer("child")};original.Children=[]*Previous{nil,child};child.Children=[]*Previous{original}
 var steps []string
 snapshot,err:=_newEventsHandlerPrevious0Capture([]*Previous{original},projection{loaded:full,steps:&steps});if err!=nil{t.Fatal(err)}
 got:=snapshot.byKey[_newEventsHandlerMatchKey0{Id:0,TenantId:2}]
 if got.value.Name!="retained"||!got.fields.Has("Name")||got.fields.Has("LoadedName")||got.fields.Has("Has"){t.Fatalf("bad mapping %+v",got)}
 *original.Id=9;*original.LoadedName="changed";original.LoadedValues[0]=99
 if *got.value.Id!=0||got.value.Name!="retained"||got.value.Values[0]!=3{t.Fatal("previous not detached")}
 if len(snapshot.byKey)!=2||len(steps)!=2||!strings.Contains(steps[1],"Children 1"){t.Fatalf("self provenance %v",steps)}
 unloaded,err:=_newEventsHandlerPrevious0Capture([]*Previous{{Id:pointer(int64(0)),LoadedName:pointer("not selected")}},projection{loaded:fields{"Id":true,"TenantId":true}});if err!=nil{t.Fatal(err)}
 omitted:=unloaded.byKey[_newEventsHandlerMatchKey0{}];if omitted.value.Name!=""||omitted.fields.Has("Name"){t.Fatal("declared mapping impersonated actual loading")}
 if _,err:=_newEventsHandlerPrevious0Capture(nil,nil);err==nil{t.Fatal("nil projection accepted")}
 repeated:=&Previous{Id:pointer(int64(6)),LoadedName:pointer("same")}
 if got,err:=_newEventsHandlerPrevious0Capture([]*Previous{repeated,repeated},projection{loaded:full});err==nil||got!=nil{t.Fatal("same pointer repeated as root escaped duplicate policy")}
 first:=&Previous{Id:pointer(int64(10)),LoadedName:pointer("first"),Children:[]*Previous{repeated}}
 second:=&Previous{Id:pointer(int64(11)),LoadedName:pointer("second"),Children:[]*Previous{repeated}}
 if got,err:=_newEventsHandlerPrevious0Capture([]*Previous{first,second},projection{loaded:full});err!=nil||len(got.byKey)!=3{t.Fatalf("shared self row rejected %v",err)}
 if got,err:=_newEventsHandlerPrevious0Capture([]*Previous{first,second},projection{loaded:full,conflict:true});err==nil||got!=nil||!strings.Contains(err.Error(),"conflicting read evidence"){t.Fatalf("conflicting evidence hidden: %v",err)}
}
type readMetadata struct{projection handler.ReadProjection}
func(m readMetadata)Projection(field string)(handler.ReadProjection,error){if field!="CurrentEvents"{return nil,fmt.Errorf("unexpected path %s",field)};return m.projection,nil}
func TestFramesOriginalIdentity(t *testing.T){
 input:=&Input{Events:[]*Record{
  {Id:pointer(int64(0)),TenantId:2,Has:&Marker{Id:true,TenantId:true}},
  {Id:pointer(int64(8)),TenantId:3,Has:&Marker{Id:true,TenantId:true}},
  {Has:&Marker{}},
 },CurrentEvents:[]*Previous{
  {Id:pointer(int64(0)),TenantId:2,LoadedName:pointer("loaded zero")},
  {Id:pointer(int64(0)),TenantId:3,LoadedName:pointer("different tenant")},
 }}
 original,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 metadata:=readMetadata{projection{loaded:fields{"Id":true,"TenantId":true,"LoadedName":true}}}
 database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,metadata);if err!=nil{t.Fatal(err)}
 // Input initialization can reorder the request and mutate the bound Current.
 *input.CurrentEvents[0].Id=99;*input.CurrentEvents[0].LoadedName="corrupted"
 *input.Events[0].Id=7;input.Events[2].Id=pointer(int64(44));input.Events[2].Has.Id=true
 input.Events[0],input.Events[2]=input.Events[2],input.Events[0]
 synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
 got,err:=database.Build(input,synced);if err!=nil{t.Fatal(err)}
 if len(got.Role0)!=3||got.Role0[0].Entity!=input.Events[0]||got.Role0[0].State.Previous!=nil||got.Role0[1].State.Previous!=nil{t.Fatalf("new/unmatched/order %+v",got.Role0)}
 prior:=got.Role0[2].State
 if prior.Previous==nil||*prior.Previous.Id!=0||prior.Previous.Name!="loaded zero"||!prior.PreviousFields.Has("Name")||prior.PreviousFields.Has("Values"){t.Fatalf("original tuple/provenance %+v",prior)}
 if !prior.Original.Has("Id")||got.Role0[0].State.Original.Has("Id"){t.Fatal("mutable identity marker used")}
}
`, frames.File, frames.Previous.File, frames.Layout.File)
}
